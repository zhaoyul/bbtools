(ns xray.metrics
  (:require [clojure.string :as str])
  (:import (java.nio.file FileSystems)
           (java.time LocalDate)
           (java.time.temporal ChronoUnit)))

(defn- churn-lines [{:keys [added deleted]}]
  (+ (long (or added 0)) (long (or deleted 0))))

(defn apply-aliases
  "Normalize :author values based on config [:authors :aliases].
   Matches exact and lower-cased names.

   Alias keys may be either author names or emails; if both exist, the first match wins:
   - author (exact)
   - author (lower-cased)
   - email (exact)
   - email (lower-cased)"
  [commits config]
  (let [aliases (get-in config [:authors :aliases] {})
        aliases-lc (into {}
                         (map (fn [[k v]] [(str/lower-case (str k)) v]))
                         aliases)
        rename (fn [{:keys [author email]}]
                 (let [author (or author "UNKNOWN")
                       email (or email "")
                       author-lc (str/lower-case (str author))
                       email-lc (str/lower-case (str email))]
                   (or (get aliases author)
                       (get aliases-lc author-lc)
                       (when-not (str/blank? email) (get aliases email))
                       (when-not (str/blank? email-lc) (get aliases-lc email-lc))
                       author)))]
    (mapv (fn [c] (assoc c :author (rename c))) commits)))

(defn timeseries-by-day
  "Return daily buckets for commits."
  [commits]
  (->> commits
       (group-by :date_day)
       (map (fn [[day cs]]
              {:date day
               :commits (count cs)
               :authors (count (set (keep :author cs)))
               :files_changed (->> cs (mapcat :files) (map :path) set count)
               :lines_added (->> cs (mapcat :files) (map :added) (reduce + 0))
               :lines_deleted (->> cs (mapcat :files) (map :deleted) (reduce + 0))}))
       (sort-by :date)
       vec))

(defn hotspots
  "Per-file hotspot stats."
  [commits]
  (let [touches (for [{:keys [date_day author files]} commits
                      {:keys [path added deleted]} files]
                  {:path path
                   :author author
                   ;; Use YYYY-MM-DD to keep downstream staleness/age computations simple.
                   :date_day date_day
                   :churn (churn-lines {:added added :deleted deleted})})]
    (->> touches
         (group-by :path)
         (map (fn [[path ts]]
                {:path path
                 :change_count (count ts)
                 :churn_lines (reduce + 0 (map :churn ts))
                 :last_touched_at (->> ts (map :date_day) sort last)}))
         (sort-by (juxt (comp - :change_count) (comp - :churn_lines) :path))
         vec)))

(defn ownership-long
  "Long-table ownership data: one row per (path, author)."
  [commits]
  (let [touches (for [{:keys [author date_day files]} commits
                      {:keys [path added deleted]} files]
                  {:path path
                   :author (or author "UNKNOWN")
                   :date_day date_day
                   :churn (churn-lines {:added added :deleted deleted})})
        totals (->> touches
                    (group-by :path)
                    (map (fn [[p rows]]
                           [p (reduce + 0 (map :churn rows))]))
                    (into {}))]
    (->> touches
         (group-by (juxt :path :author))
         (map (fn [[[path author] rows]]
                (let [c (reduce + 0 (map :churn rows))
                      last-touched (->> rows (map :date_day) sort last)
                      t (max 1 (get totals path 1))]
                  {:path path
                   :author author
                   :churn_lines c
                   :churn_pct (double (/ c t))
                   :last_touched_at last-touched})))
         (sort-by (juxt :path (comp - :churn_lines) :author))
         vec)))

(defn apply-excludes
  "Filter commits/files based on config exclusions.
   config keys used:
   - [:exclude :paths] list of substrings to exclude
   - [:exclude :globs] list of glob patterns (e.g. \"**/*.svg\")
   - [:exclude :commits] set of sha prefixes to exclude"
  [commits config]
  (let [ex-paths (get-in config [:exclude :paths] [])
        ex-globs (get-in config [:exclude :globs] [])
        ex-commits (get-in config [:exclude :commits] #{})
        nio-fs (FileSystems/getDefault)
        sep (.getSeparator nio-fs)
        compile-matcher (fn [g]
                          (try
                            (.getPathMatcher nio-fs (str "glob:" g))
                            (catch Exception _ nil)))
        glob-candidates (fn [g]
                          (let [g (str g)
                                g1 (str/replace g "\\" sep)
                                g2 (str/replace g1 "/" sep)]
                            (distinct (remove str/blank? [g g1 g2]))))
        matchers (->> ex-globs
                      (mapcat glob-candidates)
                      (map compile-matcher)
                      (remove nil?)
                      vec)
        excluded-sha? (fn [sha]
                        (some (fn [pfx] (str/starts-with? sha pfx)) ex-commits))
        excluded-path? (fn [path]
                         (some (fn [sub] (str/includes? path sub)) ex-paths))]
    (->> commits
         (remove (fn [{:keys [sha]}] (and sha (excluded-sha? sha))))
         (map (fn [c]
                (update c :files (fn [file-stats]
                                   (vec
                                    (remove (fn [{:keys [path]}]
                                              (and path
                                                   (or (excluded-path? path)
                                                       (let [parts (str/split path #"/+")
                                                             p (if (seq parts)
                                                                 (.getPath nio-fs (first parts) (into-array String (rest parts)))
                                                                 (.getPath nio-fs "" (make-array String 0)))]
                                                         (some #(.matches % p) matchers)))))
                                            file-stats))))))
         (remove (fn [{:keys [files]}] (empty? files)))
         vec)))

(defn hotspots-by-dir
  "Aggregate hotspots to directory level.
   `depth` controls how many path segments to keep (1 => top-level dir)."
  ([hotspots] (hotspots-by-dir hotspots 2))
  ([hotspots depth]
   (let [depth (max 1 (long depth))
         dir-of (fn [path]
                  (let [parts (str/split (or path "") #"/+")
                        kept (take depth (butlast parts))]
                    (if (seq kept)
                      (str/join "/" kept)
                      ".")))]
     (->> hotspots
          (group-by (comp dir-of :path))
          (map (fn [[dir rows]]
                 {:dir dir
                  :file_count (count rows)
                  :change_count (reduce + 0 (map :change_count rows))
                  :churn_lines (reduce + 0 (map :churn_lines rows))}))
          (sort-by (juxt (comp - :change_count) (comp - :churn_lines) :dir))
          vec))))

(defn- normalize-01
  "Min-max normalize to [0,1]. If all values equal, return 0 for all."
  [xs]
  (let [xs (vec xs)
        mn (apply min xs)
        mx (apply max xs)
        denom (double (- mx mn))]
    (if (zero? denom)
      (repeat (count xs) 0.0)
      (mapv (fn [x] (double (/ (- x mn) denom))) xs))))

(defn risk
  "Compute per-file risk score combining churn, complexity, and ownership dispersion.
   Returns vector of:
   {:path ... :churn_score ... :cc_score ... :ownership_score ... :risk_score ...}
   Notes:
   - churn_score is based on hotspots :change_count and :churn_lines.
   - cc_score is based on sum cc per file from complexity-functions.
   - ownership_score is (1 - top1_pct) from ownership_long."
  [hotspots complexity-functions ownership-long config]
  (let [{:keys [w-churn w-cc w-ownership]}
        (merge {:w-churn 0.45 :w-cc 0.35 :w-ownership 0.20}
               (get-in config [:metrics :risk] {}))
        churn-map (->> hotspots
                       (map (fn [{:keys [path change_count churn_lines]}]
                              [path {:change_count (long change_count)
                                     :churn_lines (long churn_lines)}]))
                       (into {}))
        cc-map (->> complexity-functions
                    (group-by :path)
                    (map (fn [[p rows]]
                           [p (reduce + 0 (map :cc rows))]))
                    (into {}))
        top1-map (->> ownership-long
                      (group-by :path)
                      (map (fn [[p rows]]
                             (let [top1 (apply max (map :churn_pct rows))]
                               [p top1])))
                      (into {}))
        paths (->> hotspots (map :path) vec)
        churn-raw (mapv (fn [p]
                          (let [{:keys [change_count churn_lines]} (get churn-map p)]
                            (+ (* 2.0 change_count) (* 0.001 churn_lines))))
                        paths)
        cc-raw (mapv (fn [p] (double (get cc-map p 0))) paths)
        owner-raw (mapv (fn [p] (double (- 1.0 (double (or (get top1-map p) 1.0))))) paths)
        churn-n (normalize-01 churn-raw)
        cc-n (normalize-01 cc-raw)
        owner-n (normalize-01 owner-raw)]
    (->> (mapv (fn [p cs ccs os]
                 {:path p
                  :churn_score cs
                  :cc_score ccs
                  :ownership_score os
                  :risk_score (+ (* w-churn cs) (* w-cc ccs) (* w-ownership os))
                  :change_count (get-in churn-map [p :change_count])
                  :churn_lines (get-in churn-map [p :churn_lines])
                  :cc_sum (get cc-map p 0)
                  :top1_pct (or (get top1-map p) 1.0)})
               paths churn-n cc-n owner-n)
         (sort-by :risk_score >)
         vec)))

(defn staleness
  "Per-file staleness based on hotspot last_touched_at vs until-day (YYYY-MM-DD).
   Returns rows:
   {:path :age_days :last_touched_at :change_count :churn_lines}"
  [hotspots until-day]
  (let [until (when (seq (str until-day))
                (try (LocalDate/parse (str until-day)) (catch Exception _ nil)))]
    (->> (or hotspots [])
         (map (fn [{:keys [path last_touched_at change_count churn_lines]}]
                (let [age (when (and until (seq (str last_touched_at)))
                            (try (.between ChronoUnit/DAYS (LocalDate/parse (str last_touched_at)) until)
                                 (catch Exception _ nil)))]
                  {:path path
                   :age_days (when (some? age) (long age))
                   :last_touched_at last_touched_at
                   :change_count (long (or change_count 0))
                   :churn_lines (long (or churn_lines 0))})))
         (sort-by (juxt (comp - (fn [r] (or (:age_days r) -1)))
                        (comp - :change_count)
                        :path))
         vec)))

(defn knowledge-loss
  "Per-file knowledge-loss: how long since the top author last touched the file.
   Depends on ownership-long including :last_touched_at per (path, author) row.
   Returns rows:
   {:path :top_author :top1_pct :last_seen :loss_days :change_count :churn_lines}"
  [hotspots ownership-long until-day]
  (let [until (when (seq (str until-day))
                (try (LocalDate/parse (str until-day)) (catch Exception _ nil)))
        hs-map (into {} (map (juxt :path identity)) (or hotspots []))
        top-row (->> (or ownership-long [])
                     (reduce (fn [m {:keys [path churn_pct churn_lines] :as row}]
                               (if-not (seq (str path))
                                 m
                                 (let [cur (get m path)
                                       cp (double (or churn_pct 0.0))
                                       curp (double (or (:churn_pct cur) 0.0))
                                       cl (long (or churn_lines 0))
                                       curcl (long (or (:churn_lines cur) 0))]
                                   (if (or (nil? cur)
                                           (> cp curp)
                                           (and (= cp curp) (> cl curcl)))
                                     (assoc m path row)
                                     m))))
                             {}))]
    (->> (keys top-row)
         (map (fn [p]
                (let [{:keys [author churn_pct last_touched_at]} (get top-row p)
                      hs (get hs-map p)
                      last-seen last_touched_at
                      loss (when (and until (seq (str last-seen)))
                             (try (.between ChronoUnit/DAYS (LocalDate/parse (str last-seen)) until)
                                  (catch Exception _ nil)))]
                  {:path p
                   :top_author author
                   :top1_pct (double (or churn_pct 0.0))
                   :last_seen last-seen
                   :loss_days (when (some? loss) (long loss))
                   :change_count (long (or (:change_count hs) 0))
                   :churn_lines (long (or (:churn_lines hs) 0))})))
         (sort-by (juxt (comp - (fn [r] (or (:loss_days r) -1)))
                        (comp - :change_count)
                        :path))
         vec)))

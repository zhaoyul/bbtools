(ns xray.cli
  (:require [babashka.cli :as cli]
            [babashka.fs :as fs]
            [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [xray.config :as config]
            [xray.git :as git]
            [xray.metrics :as metrics]
            [xray.metrics.complexity :as complexity]
            [xray.metrics.coupling :as coupling]
            [xray.report.html :as rpt])
  (:import (java.time LocalDateTime ZoneId)
           (java.time.format DateTimeFormatter)))

(defn- now-ts []
  (.format (DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss")
           (LocalDateTime/now (ZoneId/systemDefault))))

(def cli-spec
  {:repo {:desc "Repo root" :alias :r :coerce :string}
   :since {:desc "Since date (YYYY-MM-DD)" :coerce :string}
   :until {:desc "Until date (YYYY-MM-DD)" :coerce :string}
   :branch {:desc "Branch" :coerce :string}
   :all {:desc "Use --all" :coerce :boolean}
   :no-merges {:desc "Exclude merge commits" :coerce :boolean}
   :path {:desc "Subdir path filter" :coerce :string}
   :config {:desc "Config file path" :coerce :string}
   :out {:desc "Output directory" :coerce :string}
   :topN {:desc "Default TopN in report" :coerce :int}
   :include-raw {:desc "Include raw commit data for interactive filtering in HTML report" :coerce :boolean}
   :vendor-dir {:desc "Directory containing vega.min.js and vega-lite.min.js" :coerce :string}})

(defn- detect-vendor-dir
  "Try to locate vendor dir.
   Works both when run from this repo and when installed via bbin (cwd can be anywhere)."
  []
  (let [cwd (fs/cwd)
        bb-config-path (some-> (System/getProperty "babashka.config") fs/path)
        from-bb-config (when bb-config-path
                         ;; If we're running with `bb --config /path/to/tools/xray/bb.edn`, use that as anchor.
                         (let [root (fs/parent bb-config-path)
                               vend (when root (fs/path root "vendor"))]
                           (when (and vend (fs/exists? vend)) (str vend))))
        from-bb-config-edn (when (and bb-config-path (fs/exists? bb-config-path))
                             ;; When installed via bbin, babashka.config points to a temp file like:
                             ;; {:deps {local/deps {:local/root "<script-root>"}}}
                             (try
                               (let [m (edn/read-string (slurp (str bb-config-path)))
                                     deps (:deps m)
                                     local-root (some (fn [[_ coord]]
                                                        (when (map? coord)
                                                          (or (:local/root coord)
                                                              (:local/root (get coord :local)))))
                                                      deps)
                                     vend (when local-root (fs/path local-root "vendor"))]
                                 (when (and vend (fs/exists? vend)) (str vend)))
                               (catch Exception _ nil)))
        ;; /.../tools/xray/src/xray/cli.clj -> /.../tools/xray/vendor
        from-source (some-> *file*
                            fs/path
                            fs/parent
                            fs/parent
                            fs/parent
                            (fs/path "vendor"))
        candidates (remove nil?
                           [from-bb-config-edn
                            from-bb-config
                            from-source
                            (fs/path cwd "vendor")
                            (fs/path cwd "tools" "xray" "vendor")])]
    (some (fn [p] (when (and p (fs/exists? p)) (str p))) candidates)))

(defn- usage []
  (str
   "xray (standalone)\n\n"
   "Commands:\n"
   "  report  Generate HTML report directory (offline) + data.json\n"
   "  export  Export data.json only\n\n"
   "Examples:\n"
   "  xray report --repo .. --since 2025-01-01 --until 2026-02-08 --out ./out\n"
   "  xray export --repo .. --since 2025-01-01 --until 2026-02-08 --out ./data.json\n"))

(defn- write-json! [path data]
  (fs/create-dirs (fs/parent path))
  (spit (str path) (json/generate-string data {:pretty true})))

(defn- build-data
  [{:keys [repo since until branch all no-merges path config topN include-raw]
    :or {all true no-merges true topN 30 include-raw false}}]
  (let [repo (or repo ".")
        cfg (config/load-config {:repo repo :config config})
        commits0 (git/read-commits {:repo repo
                                        :all? all
                                        :branch branch
                                        :no-merges? no-merges
                                        :since since
                                        :until until
                                        :path path
                                        :exclude-pathspecs (get-in cfg [:exclude :git-pathspecs])})
        commits (-> commits0
                    (metrics/apply-aliases cfg)
                    (metrics/apply-excludes cfg))
        timeseries (metrics/timeseries-by-day commits)
        hotspots (metrics/hotspots commits)
        hotspots-dirs (metrics/hotspots-by-dir hotspots 2)
        ownership-long (metrics/ownership-long commits)
        until-day (or until (->> commits (keep :date_day) sort last) "")
        staleness (metrics/staleness hotspots until-day)
        knowledge-loss (metrics/knowledge-loss hotspots ownership-long until-day)
        ;; Compute complexity only for likely relevant Clojure files (top hotspots).
        top-hotspot-paths (->> hotspots (take 200) (map :path) vec)
        complexity-functions (complexity/complexity-functions repo top-hotspot-paths)
        coupling-res (coupling/temporal-coupling commits hotspots cfg)
        risk (metrics/risk hotspots complexity-functions ownership-long cfg)
        risk-defaults (merge {:w-churn 0.45 :w-cc 0.35 :w-ownership 0.20}
                             (get-in cfg [:metrics :risk] {}))
        coupling-defaults (merge {:min-cochange 3 :topK 25 :topN 100}
                                 (get-in cfg [:metrics :coupling] {}))
        raw-commits (when include-raw
                      (->> commits
                           (mapv (fn [{:keys [sha author date_day files]}]
                                   {:sha sha
                                    :author (or author "UNKNOWN")
                                    :date_day date_day
                                    :files (mapv (fn [{:keys [path added deleted]}]
                                                   {:path path :added added :deleted deleted})
                                                 (or files []))}))))
        raw-authors (when (seq raw-commits)
                      (->> raw-commits (map :author) distinct sort vec))
        raw-days (when (seq raw-commits)
                   (->> raw-commits (map :date_day) sort vec))
        raw-min-day (first raw-days)
        raw-max-day (last raw-days)]
    {:schema_version "1.0"
     :repo {:root (str (fs/absolutize repo))
            :head (try (-> (p/sh {:dir repo} "git" "rev-parse" "HEAD") :out str/trim)
                       (catch Exception _ nil))}
     :params {:since since
              :until until
              :branch branch
              :all (boolean all)
              :no_merges (boolean no-merges)
              :path path
              :topN topN}
     :ui_defaults {:risk risk-defaults
                   :coupling coupling-defaults}
     :report (:report cfg)
     :timeseries timeseries
     :hotspots hotspots
     :hotspots_dirs hotspots-dirs
     :ownership_long ownership-long
     :staleness staleness
     :knowledge_loss knowledge-loss
     :complexity_functions complexity-functions
     :coupling_pairs (:pairs coupling-res)
     :coupling_pairs_long (:pairs_long coupling-res)
     :coupling_matrix (:matrix coupling-res)
     :risk risk
     :raw (when include-raw
            {:commits raw-commits
             :authors raw-authors
             :min_day raw-min-day
             :max_day raw-max-day})}))

(defn- cmd-export [opts]
  (let [opts (if (nil? (:include-raw opts)) (assoc opts :include-raw false) opts)
        data (build-data opts)
        out (or (:out opts) (str "xray-data-" (now-ts) ".json"))]
    (write-json! out data)
    {:ok true :out out}))

(defn- cmd-report [opts]
  (let [opts (if (nil? (:include-raw opts)) (assoc opts :include-raw true) opts)
        data (build-data opts)
        out-dir (or (:out opts) (str "xray-report-" (now-ts)))
        vendor-dir (or (:vendor-dir opts) (detect-vendor-dir))]
    (when-not vendor-dir
      (throw (ex-info "vendor-dir not found; pass --vendor-dir" {:cwd (str (fs/cwd))})))
    (rpt/write-report! {:out-dir out-dir
                        :data data
                        :title (get-in data [:report :title])
                        :vendor-dir vendor-dir})
    (write-json! (fs/path out-dir "meta.json")
                 {:generated_at (str (LocalDateTime/now))
                  :schema_version (:schema_version data)
                  :params (:params data)})
    {:ok true :out (str out-dir)}))

(defn -main
  [& args]
  (let [{:keys [args opts]} (cli/parse-args args {:spec cli-spec})
        cmd (first args)]
    (case cmd
      "report" (cmd-report opts)
      "export" (cmd-export opts)
      (do
        (println (usage))
        (when cmd
          (println "Unknown command:" cmd))
        {:ok false :error :usage}))))

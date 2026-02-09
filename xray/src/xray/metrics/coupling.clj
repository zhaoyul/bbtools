(ns xray.metrics.coupling
  (:require [clojure.string :as str]))

(defn- combos
  "All unique pairs (i<j) from vector v."
  [v]
  (let [n (count v)]
    (loop [i 0 acc []]
      (if (>= i n)
        acc
        (recur (inc i)
               (loop [j (inc i) acc2 acc]
                 (if (>= j n)
                   acc2
                   (recur (inc j) (conj acc2 [(nth v i) (nth v j)])))))))))

(defn temporal-coupling
  "Compute temporal coupling among top-K hotspot files.
   Returns {:pairs [...], :matrix [...]}.
   config keys used:
   - [:metrics :coupling :min-cochange] (default 3)
   - [:metrics :coupling :topK] (default 25) ; matrix size
   - [:metrics :coupling :topN] (default 100) ; pairs output size"
  [commits hotspots config]
  (let [{:keys [min-cochange topK topN]}
        (merge {:min-cochange 3 :topK 25 :topN 100}
               (get-in config [:metrics :coupling] {}))
        ;; File touch counts from hotspots
        touch-count (into {}
                          (map (fn [{:keys [path change_count]}] [path (long change_count)]))
                          hotspots)
        top-files (->> hotspots (take (max 1 (long topK))) (map :path) vec)
        top-set (set top-files)
        ;; accumulate co-change counts
        counts (reduce
                (fn [m {:keys [files]}]
                  (let [fs (->> files (map :path) distinct (filter top-set) vec)
                        pairs (combos fs)]
                    (reduce (fn [m2 [a b]]
                              (let [k (if (neg? (compare a b)) [a b] [b a])]
                                (update m2 k (fnil inc 0))))
                            m
                            pairs)))
                {}
                commits)
        pairs (->> counts
                   (map (fn [[[a b] c]]
                          (let [ta (max 1 (get touch-count a 1))
                                tb (max 1 (get touch-count b 1))
                                support (/ (double c) (double (min ta tb)))]
                            {:a a :b b :co_change_count c :support_pct support})))
                   (filter (fn [{:keys [co_change_count]}] (>= co_change_count (long min-cochange))))
                   (sort-by (juxt (comp - :co_change_count) (comp - :support_pct) :a :b))
                   (take (max 1 (long topN)))
                   vec)
        ;; Long form for "selected file -> coupled files" views.
        pairs-long (->> pairs
                        (mapcat (fn [{:keys [a b co_change_count support_pct]}]
                                  [{:path a :other b :co_change_count co_change_count :support_pct support_pct}
                                   {:path b :other a :co_change_count co_change_count :support_pct support_pct}]))
                        vec)
        ;; matrix: include both directions for heatmap
        matrix (->> counts
                    (mapcat (fn [[[a b] c]]
                              (when (and (top-set a) (top-set b))
                                [{:a a :b b :co_change_count c}
                                 {:a b :b a :co_change_count c}])))
                    (remove nil?)
                    vec)]
    {:pairs pairs
     :pairs_long pairs-long
     :matrix matrix}))

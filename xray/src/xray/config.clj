(ns xray.config
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.set :as set]))

(defn- deep-merge
  "Recursively merge maps."
  [& xs]
  (letfn [(dm [a b]
            (cond
              (and (map? a) (map? b))
              (merge-with dm a b)
              (and (set? a) (set? b))
              (set/union a b)
              (and (sequential? a) (sequential? b))
              (->> (concat a b) distinct vec)
              :else b))]
    (reduce dm {} xs)))

(def default-config
  {:authors {:aliases {}}
   :exclude {:paths ["node_modules/" "target/" ".shadow-cljs/" ".clj-kondo/" ".clj-kondo"]
             ;; Git pathspec exclusions applied at `git log` time (performance + "hard exclude").
             ;; Strings here are passed as `:(exclude,glob)<pattern>`.
             :git-pathspecs [".clj-kondo/**" ".clj-kondo"]
             :globs  []
             :commits #{}}
   :classify {:doc-ext #{".md" ".org"}
              :code-ext #{".clj" ".cljs" ".cljc" ".js" ".ts"}}
   :metrics {:coupling {:min-cochange 3 :topN 100}
             :hotspots {:topN 200}
             :risk {:w-churn 0.45 :w-cc 0.35 :w-ownership 0.20}}
   :report {:title "XRay Report"
            :theme "light"}})

(defn- read-edn-file [p]
  (-> p slurp edn/read-string))

(defn load-config
  "Load xray.edn if present; merge into defaults.
   opts:
   - :repo   repo root dir
   - :config explicit config path (optional)"
  [{:keys [repo config]}]
  (let [repo (or repo ".")
        cfg-path (or config (str (fs/path repo "xray.edn")))]
    (if (fs/exists? cfg-path)
      (deep-merge default-config (read-edn-file cfg-path))
      default-config)))

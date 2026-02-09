(ns xray.git
  (:require [babashka.process :as p]
            [clojure.string :as str])
  (:import (java.time OffsetDateTime)))

(def ^:private commit-marker "__XRAY_COMMIT__")

(defn- sh
  "Run command in repo dir. Throws on non-zero exit."
  [repo & args]
  (let [{:keys [exit out err]} (apply p/sh {:dir repo} args)]
    (when-not (zero? exit)
      (throw (ex-info "command failed"
                      {:repo repo :args args :exit exit :err err :out out})))
    out))

(defn- normalize-path [s]
  ;; Keep repo-relative paths consistent across OS.
  (-> s (str/replace "\\" "/")))

(defn- parse-numstat-line [line]
  ;; Format: added \t deleted \t path
  ;; For binary changes, added/deleted are '-' which we skip for now.
  (when-let [[_ a d path] (re-matches #"^([0-9-]+)\t([0-9-]+)\t(.*)$" line)]
    (when-not (or (= a "-") (= d "-"))
      {:added (Long/parseLong a)
       :deleted (Long/parseLong d)
       :path (normalize-path path)})))

(defn- parse-commit-header [line]
  ;; __XRAY_COMMIT__|sha|author|email|date
  (let [[marker sha author email date] (str/split line #"\|" 5)]
    (when (= marker commit-marker)
      {:sha sha
       :author author
       :email email
       :date date
       :date_day (str (.toLocalDate (OffsetDateTime/parse date)))
       :files []})))

(defn read-commits
  "Read commits from git log with numstat.
   opts:
   - :repo
   - :all? (default true)
   - :branch (optional)
   - :no-merges? (default true)
   - :since (YYYY-MM-DD, optional)
   - :until (YYYY-MM-DD, optional)
   - :path  (subdir filter, optional; passed to git as pathspec)
   - :exclude-pathspecs (vector of git pathspecs to exclude; applied at git-time)"
  [{:keys [repo all? branch no-merges? since until path exclude-pathspecs]
    :or {all? true no-merges? true}}]
  (let [repo (or repo ".")
        base ["git" "log"
              "--date=iso-strict"
              (str "--pretty=format:" commit-marker "|%H|%an|%ae|%ad")
              "--numstat"]
        range (cond
                all? ["--all"]
                branch [branch]
                :else [])
        merges (if no-merges? ["--no-merges"] [])
        time-args (concat
                   (when since [(str "--since=" since)])
                   (when until [(str "--until=" until)]))
        ex (->> (or exclude-pathspecs [])
                (map str)
                (remove str/blank?)
                vec)
        include (cond
                  (and path (not (str/blank? path))) [path]
                  (seq ex) ["."]
                  :else nil)
        exclude (when (seq ex)
                  (mapv (fn [p] (str ":(exclude,glob)" p)) ex))
        pathspec (when (or include exclude)
                   (vec (concat ["--"] (or include []) (or exclude []))))
        cmd (vec (concat base range merges time-args pathspec))
        out (apply sh repo cmd)
        lines (str/split-lines out)]
    (loop [xs lines
           cur nil
           acc []]
      (if (empty? xs)
        (cond-> acc cur (conj cur))
        (let [line (first xs)]
          (if-let [hdr (parse-commit-header line)]
            (recur (rest xs)
                   hdr
                   (cond-> acc cur (conj cur)))
            (let [f (parse-numstat-line line)]
              (recur (rest xs)
                     (cond-> cur f (update :files conj f))
                     acc))))))))

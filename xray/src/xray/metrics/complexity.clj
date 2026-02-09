(ns xray.metrics.complexity
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [rewrite-clj.parser :as p]
            [rewrite-clj.node :as node]))

(def ^:private clj-exts #{"clj" "cljs" "cljc"})

(def ^:private valid-def-forms
  #{'defn 'defn- 'defstate})

(def ^:private cc-inc-forms
  ;; Minimal set; can expand later.
  #{'if 'when 'cond 'case 'when-let 'if-let 'when-some})

(defn- clj-file? [path]
  (when-let [ext (fs/extension path)]
    (contains? clj-exts (str/lower-case ext))))

(defn- node-type
  "Return first symbol of list-like node, or nil."
  [n]
  (try
    (-> n :children first :value)
    (catch Exception _ nil)))

(defn- non-ws-children [n]
  (->> (:children n)
       (remove node/whitespace?)
       (remove node/comment?)))

(defn- defn-node?
  [n]
  (contains? valid-def-forms (node-type n)))

(defn- fn-name
  [defn-node]
  (try
    (let [c (non-ws-children defn-node)
          nm (second c)]
      (when nm
        (str (node/sexpr nm))))
    (catch Exception _ nil)))

(declare node-cc)

(defn- cc-inc?
  [n]
  (contains? cc-inc-forms (node-type n)))

(defn- node-cc
  "Compute cyclomatic complexity increments within node (not counting base 1)."
  [n]
  (let [kids (or (:children n) [])]
    (reduce
     (fn [r k]
       (+ r (node-cc k)))
     (if (cc-inc? n) 1 0)
     kids)))

(defn- analyze-file [repo path]
  (let [abs (str (fs/path repo path))]
    (when (and (clj-file? path) (fs/exists? abs))
      (try
        (let [code (slurp abs)
              parsed (p/parse-string-all code)
              top (filter defn-node? (:children parsed))]
          (->> top
               (mapv (fn [n]
                       (let [nm (fn-name n)
                             cc (+ 1 (node-cc n))]
                         (when nm
                           {:path path :fn nm :cc cc :lang "clojure"}))))
               (remove nil?)
               vec))
        (catch Exception _e
          ;; Skip parse errors; keep pipeline running.
          [])))))

(defn complexity-functions
  "Compute complexity per function for a list of repo-relative paths."
  [repo paths]
  (->> paths
       (distinct)
       (mapcat #(analyze-file repo %))
       vec))

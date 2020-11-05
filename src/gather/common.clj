(ns gather.common
  (:require
    [clojure.string :as str]
    [clojure.core.async :as a]
    [clojure.java.shell :as sh]
  ))

(defn lower
  "Convert name to lowercase and '-' to '_'"
  [name]
  (clojure.string/lower-case (clojure.string/replace name "-" "_")))

(defn comma-join
  "Join items with comma"
  [items]
  (clojure.string/join ", " items))

(def table-types {
  :t "trades"
  :b "buy"
  :s "sell"
  })

(defn wc-test
  "Wildcard test"
  [wc]
  (cond
    (= wc "*") (fn [s] true)
    (str/starts-with? wc "*") (fn [s] (str/ends-with? s (subs wc 1)))
    ))

(defn get-table-name
  "Get table name for trades or depths"
  [market pair type]
  (str "fx." (lower market) "_" (lower pair) "_" (type table-types)))

(defn try-loop
  "Try to call function in loop"
  [title func]
  (loop []
    (try
       (func)
       (catch Exception e
         (println "\n" title "exception:")
         (clojure.stacktrace/print-stack-trace e)))
    (Thread/sleep 1000)
    (recur)))

(defn forever-loop
  "Execute function in loop"
  [title func]
  (a/thread (try-loop title func)))

(defn now
  "Return current time"
  []
  (new java.sql.Timestamp (System/currentTimeMillis)))

(defn throttle
  [ms f!]
  (let [last (atom 0)]
    (fn [& args]
      (let [t (System/currentTimeMillis)]
        (if (> t @last)
          (do (apply f! args) (reset! last (+ ms t))))))))

(defn atom-map-sum
  "aggregate "
  [f m]
  (reduce-kv
    (fn [m k v]
      (let [kr (f k)]
        (assoc m kr (+ @v (get m kr 0)))))
    {} m))

(defn vec-to-map-of-vec
  "Convert vector of records to map of vectors"
  [key value coll]
  (reduce
    (fn [coll item]
      (let [kr (key item)]
        (assoc coll kr (conj (get coll kr []) (value item)))))
    {} coll))

(defn filter-keys
  [f coll]
  (reduce-kv
    (fn [coll k v]
      (if (f k) (assoc coll k v) coll)
      {} coll)))

(defn to-uint [s] (Integer/parseUnsignedInt s))

(defn within
  [a b v]
  (and
    (>= v a)
    (<= v b)))

(defn exec!
  [& args]
  (let [{exit :exit err :err out :out} (apply sh/sh args)]
    (if (not= exit 0) (throw (Exception. err)) out)))

(defn pwd "Current directory" [] (-> "pwd" exec! str/trim))

(defn ls [& args] (str/split-lines (apply exec! "ls" args)))

(defn extract! [archive destination]
  (exec! "tar" "-xvf" archive :dir destination))

(defn mv! [dst & src]
  (apply exec! "mv" "-t" dst src))

(defn mv-all! [src dst]
  (->> src ls (map (partial str src "/")) (apply mv! dst)))

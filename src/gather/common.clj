(ns gather.common
  (:require
    [clojure.core.async :as a]
  ))

(defn lower
  "Convert name to lowercase and '-' to '_'"
  [name]
  (clojure.string/lower-case (clojure.string/replace name "-" "_")))

(defn comma-join
  "Join items with comma"
  [items]
  (clojure.string/join ", " items))

(defn trades-table-name
  "Get table name for trades"
  [market pair]
  (str "fx." (lower market) "_" (lower pair) "_trades"))

(defn depths-table-name
  "Get table name for depths"
  [market pair buy]
  (str "fx." (lower market) "_" (lower pair) "_" (if buy "buy" "sell")))

(defn try-loop
  "Try to call function in loop"
  [title func]
  (loop []
    (try
       (func)
       (catch Exception e (println "\n" title " exception: " (.getMessage e))))
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

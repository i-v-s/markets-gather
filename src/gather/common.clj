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

(def table-types {
  :t "trades"
  :b "buy"
  :s "sell"
  })

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

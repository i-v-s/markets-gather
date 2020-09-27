(ns gather.common
  )

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

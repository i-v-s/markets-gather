(ns gather.check
  (:require [clojure.string :as str]
            [house.sql      :as sql]
            [house.ch       :as ch]
            [gather.common  :as c]))

(defn parse-type
  [table-name]
  (-> (re-find #"^\w+_([a-z]+)$" table-name)
      second
      #{"candles" "trades" "buy" "sell"}
      keyword))

(defn parse-candle
  [table-name]
  (let [[_ market quote tf'] (re-find #"^([a-z0-9_]+)_([a-z0-9]+)_([a-z0-9]+)_candles$" table-name)
        tf (keyword tf')]
    [market quote (- (c/intervals-map tf)) tf]))

(defn str-interval
  [[a b]]
  (if (= a b)
    a
    (str a " - " b)))

(defn check-gaps
  [c table field ff & {:keys [render] :or {render str}}]
  (let [ts (= field "time")
        items (->> (str "SELECT " field " FROM " table " GROUP BY " field " ORDER BY " field)
                   (ch/exec! c)
                   ch/fetch-all
                   (map (if ts (comp c/ts-to-seconds first) first)))
        gaps (c/ajacent-filter ff items)
        periods (->> gaps
                     (apply concat)
                     (filter some?)
                     (map render)
                     (partition 2)
                     (map str-interval))]
    (if (not-empty items)
      (str
       "rows: " (count items)
       "; periods: " (count periods) " [" (c/comma-join periods) "]")
      "EMPTY")))

(defn check-asset-gaps
  [conn table field tf]
  (->> [(sql/select
         ["p.time" :as "l_time" "d.time" :as "f_time"]
         :with {"filtered" (->> (sql/select ["time" (str "MAX(" field ")") :as "present"]
                                            :from table
                                            :group-by "time"
                                            :order-by "time")
                                (sql/enumerate "n1")
                                (sql/where "present > 0")
                                (sql/enumerate "n2"))}
         :from [(sql/select ["time" "n2"]
                            :from "filtered"
                            :where "runningDifference(n1) != 1") :as "d"
                :left-join "filtered" :as "p"
                :on "p.n2 + 1 = d.n2"])
        (sql/select ["MAX(time)" "FROM_UNIXTIME(0)"] :from table :where (str field " > 0"))]
       (apply sql/union-all)
       (sql/order-by "l_time")
       (ch/fetch-all conn)
       (apply concat)
       rest
       (map (partial c/format-ts tf))
       (partition 2)
       (map str-interval)
       ))

(defn -main
  "Start with params"
  [db-url args verbosity]
  (let [conn (ch/connect db-url)
        tabs (sort (ch/fetch-tables conn))
        f-tabs (if (empty? args)
                 tabs
                 (filter (apply c/wildcard-checker args) tabs))
        {candle-tables :candles
         trades :trades
         buy :buy
         sell :sell
         other nil} (group-by parse-type f-tabs)]
    (when (not-empty other)
      (println "Unknown tables:" (c/comma-join other)))
    (doseq [:let [table-map (zipmap candle-tables
                                    (map parse-candle candle-tables))]
            table (sort-by table-map candle-tables)
            :let [tf (last (table-map table))
                  gap (c/intervals-map tf)]]
      (assert gap)
      (println table
               (check-gaps conn table "time"
                           (fn [a b]
                             (< (+ a gap) b))
                           :render (comp (partial c/format-ts tf) c/make-ts-sec)))
      (when (> verbosity 0)
        (doseq [asset-col (->> table (ch/fetch-cols conn) (filter #(str/ends-with? % "_open")) sort)]
          (println (str/upper-case (c/re-one #"(\w+)_open" asset-col)) (c/comma-join (check-asset-gaps conn table asset-col tf))))))))

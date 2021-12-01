(ns gather.check
    (:require
   [clojure.string :as str]
   [gather.common :as c]
   [gather.ch :as ch]))

(defn get-type
  [table-name]
    (-> (re-find #"\w+_([a-z]+)" table-name)
        second
        #{"candles" "trades" "buy" "sell"}
        keyword
        ))

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

(defn render-month
  [ts]
  (let [dt (.toLocalDateTime (c/make-ts-sec ts))]
    (format "%4d-%02d" (.getYear dt) (.getValue (.getMonth dt)))))

(defn render-day
  [ts]
  (let [dt (.toLocalDateTime (c/make-ts-sec ts))]
    (format "%4d-%02d-%02d" (.getYear dt) (.getValue (.getMonth dt)) (.getDayOfMonth dt))))


(def ts-render
  {:1mo render-month
   :1w render-day
   :3d render-day
   :1d render-day})

(defn -main
  "Start with params"
  [db-url args]
  (let [conn (ch/connect db-url)
        tabs (sort (ch/fetch-tables conn))
        f-tabs (if (empty? args)
                 tabs
                 (filter (apply c/wildcard-checker args) tabs))
        {candles :candles
         trades :trades
         buy :buy
         sell :sell
         other nil} (group-by get-type f-tabs)]
    (when (not-empty other)
      (println "Unknown tables:" (c/comma-join other)))
    (doseq [table candles
            :let [tf (->> table
                           (re-find #"\w+_([a-z0-9]+)_candles")
                           second
                           keyword)
                  gap (c/intervals-map tf)]]
      (assert gap)
      (println table
               (check-gaps conn table "time"
                           (fn [a b]
                             (< (+ a gap) b))
                           :render (get ts-render tf (comp str c/make-ts-sec)))))))

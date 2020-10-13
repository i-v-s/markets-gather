(ns gather.import
  (:require
    [clojure.java.io :as io]
    [clojure.data.csv :as csv]
    [gather.common :as c]
    ))


(defn take-csv
  "Takes file name and reads data."
  [fname]
  (with-open [file (io/reader fname)]
    (csv/read-csv (slurp file))))

(defn row-to-map
  "Convert csv row to id:table map"
  [row]
  [
    (Integer/parseInt (get row 0))
    (c/trades-table-name (get row 4) (get row 1))
  ])

(defn read-map
  "Reads map from csv"
  [fname]
  (into {}
    (map row-to-map (subvec (vec (take-csv fname)) 1))
  ))

(defn trades-copy-query
  "Query that copies trades to individual table"
  [pair_id table]
  (str
    "INSERT INTO " table "(id, time, buy, price, coin, base) "
    "SELECT id, time, buy, price, value? FROM main.trades "
    "WHERE pair = " pair_id))


(defn main
  "Main function"
  [fname]
  (doseq [[id table] (read-map fname)]
    (println "ID " id " TABLE " table)
  ))

(defn -main
  "Start with params"
  [arg]
  (main "/home/igor/data/fx-ch/pairs.csv")
  )

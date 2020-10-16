(ns gather.import
  (:require
    [clojure.java.io :as io]
    [clojure.data.csv :as csv]
    [gather.common :as c]
    [gather.ch :as ch]
    [gather.core :as core]
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
    [(c/trades-table-name (get row 4) (get row 1)) (get row 4) (get row 1)]
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
    "SELECT id, time, buy, price, value / price, value FROM main.trades "
    "WHERE pair = " pair_id))

(defn main
  "Main function"
  [fname db-url]
  (let [
    conn (ch/connect db-url)
    ]
    (doseq [[id [table market pair]] (read-map fname)]
      (println "ID: " id " TABLE: " table " MARKET: " market " PAIR: " pair)
      (println "Creating...")
      (core/create-market-tables conn market [pair])
      (println "Coping...")
      (ch/exec-vec! conn [(trades-copy-query id table)])
  )))

(defn -main
  "Start with params"
  [arg]
  (main "/home/igor/data/fx-ch/pairs.csv" core/ch-url)
  )

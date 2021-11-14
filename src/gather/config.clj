(ns gather.config
  (:require
    [clojure.java.io :as io]
    [clojure.data.json :as json]
    [clojure.walk :as w]
    [gather.common :as c]
    [gather.storage :as sg]
    [gather.exmo :as exmo]
    [gather.binance :as binance]
  )
  (:import [gather.storage CandlesData]
  ))

(defn exists?
  "Check if file exists"
  [file-name]
  (-> file-name io/as-file .exists))

(defn must
  [value pred message]
  (if (pred value)
    value
    (throw (Exception. (str message ": " value)))
  ))

(defn filter-exists
  [possible message coll]
  (let [{bad false good true} (group-by (partial contains? possible) coll)]
  (if (not-empty bad) (println (str message ":") bad))
  good))

(defn prepare-intervals
  [{int-map :intervals-map market :name} candles]
  (->> candles
    (map keyword)
    (filter-exists int-map (str "Unknown intervals for market " market))
    distinct (sort-by c/intervals-map >) not-empty
  ))

(defn load-markets
  [markets-config]
  (for [
    [market-key {raw-pairs :raw-pairs candles :candles}] markets-config
    :let [market (case market-key
      :exmo (exmo/create)
      :binance (binance/create)
      (println "Unknown market:" (name market-key)))]
    :when (some? market)
    :let [
      all-pairs (-> market sg/get-all-pairs set)
      filtered-raw-pairs (filter-exists all-pairs (str "Unknown pairs for market " (:name market)) raw-pairs)]]
    (assoc market
      :candles (if candles (CandlesData.
        all-pairs
        (prepare-intervals market candles)
      ) nil)
      :raw (sg/make-raw-data (:name market) filtered-raw-pairs)
  )))

(defn load-json
  "Reads and parse config.json data"
  [file-name]
  (let [to-load
    (if (some? file-name)
      file-name
      (first (filter exists? [
        "config.json"
        "config.default.json"
      ]))
    )]
    (println "Loading config file:" to-load)
    (-> to-load
      (must exists? "Config file not exists")
      slurp
      json/read-str
      w/keywordize-keys
    )
  ))

(defn load-config
  [& [file-name]]
  (let [{db :clickhouse markets :markets} (load-json file-name)] {
    :clickhouse db
    :markets (load-markets markets)}))

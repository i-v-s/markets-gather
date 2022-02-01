(ns gather.config
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :refer [info warn]]
            [clojure.walk :as w]
            [gather.common :as c]
            [gather.storage :as sg]
            [gather.exmo :as exmo]
            [gather.binance :as binance])
  (:import [gather.storage CandlesData]))

(defn must
  [value pred message]
  (if (pred value)
    value
    (throw (Exception. (str message ": " value)))
  ))

(def group-pairs
  "Group pairs by quoted asset"
  (comp
   (partial apply zipmap)
   (juxt keys
         (comp (partial mapv (partial mapv first)) vals))
   (partial group-by second)
   (partial map c/hyphen-split)))

(defn filter-exists
  [possible message items]
  (let [[good bad] (c/group-by-contains possible items)]
  (when (not-empty bad) (warn (str message ":") bad))
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
  (for [[market-key {raw-pairs :raw-pairs candles :candles cq :candle-quotes}] markets-config
        :let [market (case market-key
                       :exmo (exmo/create)
                       :binance (binance/create)
                       (warn "Unknown market:" (name market-key)))]
        :when (some? market)
        :let [all-pairs (-> market sg/get-all-pairs set)
              filtered-raw-pairs (filter-exists all-pairs (str "Unknown pairs for market " (:name market)) raw-pairs)
              groupped-pairs (group-pairs all-pairs)
              filtered-candle-pairs (if cq (select-keys groupped-pairs cq) groupped-pairs)]]
    (do
      (when (and candles (empty? filtered-candle-pairs))
        (warn "No pairs found for candle loading"))
      (assoc market
             :candles (if (and candles (not-empty filtered-candle-pairs))
                        (CandlesData. filtered-candle-pairs (prepare-intervals market candles))
                        nil)
             :raw (sg/make-raw-data (:name market) filtered-raw-pairs)))))

(defn load-json
  "Reads and parse config.json data"
  [file-name]
  (let [to-load
        (if (some? file-name)
          file-name
          (first (filter c/exists? ["config.json"
                                  "config.default.json"])))]
    (info "Loading config file:" to-load)
    (-> to-load
        (must c/exists? "Config file not exists")
        slurp
        json/read-str
        w/keywordize-keys)))

(defn load-config
  [& [file-name]]
  (let [{db :clickhouse markets :markets} (load-json file-name)]
    {:clickhouse db
     :markets (load-markets markets)}))

(ns gather.config
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.data.json :as json]
            [clojure.walk      :as w]
            [exch.utils        :as xu]
            [exch.exmo         :as exmo]
            [exch.binance.core :as binance]
            [gather.common     :as c]
            [gather.storage    :as sg])

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
                       :exmo         (exmo/create)
                       :binance      (binance/create :spot)
                       :binance-usdm (binance/create :usdm)
                       (warn "Unknown market:" (name market-key)))]
        :when (some? market)
        :let [all-pairs (-> market xu/get-all-pairs set)
              groupped-pairs (group-pairs all-pairs)
              filtered-candle-pairs (if cq (select-keys groupped-pairs cq) groupped-pairs)]]
    (do
      (when (and candles (empty? filtered-candle-pairs))
        (warn "No pairs found for candle loading"))
      (assoc market
             :candles (if (and candles (not-empty filtered-candle-pairs))
                        (CandlesData. filtered-candle-pairs (prepare-intervals market candles))
                        nil)
             :raw (some->> raw-pairs
                           (filter-exists all-pairs (str "Unknown pairs for market " (:name market)))
                           (sg/make-raw-data (:name market)))))))

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

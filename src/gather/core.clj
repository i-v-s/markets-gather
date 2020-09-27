(ns gather.core
  (:require
    [compojure.core :as compojure :refer [GET]]
    [ring.middleware.params :as params]
    [compojure.route :as route]
    [aleph.http :as http]
    [byte-streams :as bs]
    [manifold.stream :as s]
    [manifold.deferred :as d]
    [manifold.bus :as bus]
    [clojure.string :as str]
    [clojure.core.async :as a]
    [clojure.data.json :as json]
    [gather.ch :as ch]
    [gather.common :as c]))

(def trade-rec {
  :id "Int32 CODEC(Delta, LZ4)"
  :time "DateTime"
  :buy "UInt8"
  :price "Float64 CODEC(Gorilla)"
  :coin "Float32"
  :base "Float32"
  })

(defn create-market-tables-queries
  "Get queries for market tables creation"
  [market pairs]
  (concat
    ["CREATE DATABASE IF NOT EXISTS fx"]
    (map (fn [pair]
      (ch/create-table-query
        (c/trades-table-name market pair) trade-rec
      :engine "ReplacingMergeTree()" :partition-by "toYYYYMM(time)" :order-by ["id"]))
      pairs)
  ))

(defn put-trades!
  "Put trades record into Clickhouse"
  [conn market pair trades]
  (ch/insert-many! conn
    (str
      "INSERT INTO " (c/trades-table-name market pair)
      "(id, time, buy, price, coin, base) VALUES (?, ?, ?, ?, ?, ?)")
    trades
  ))

(defn exmo-trade
  "Transform Exmo trade record to Clickhouse row"
  [r]
  [
    (get r "trade_id")
    (new java.sql.Timestamp (* 1000 (get r "date")))
    (if (= "buy" (get r "type")) 1 0)
    (Float/parseFloat (get r "price"))
    (Float/parseFloat (get r "quantity"))
    (Float/parseFloat (get r "amount"))
  ])

(def ch-url "jdbc:clickhouse://127.0.0.1:9000")

(defn exmo-topic
  "Convert pair to topic"
  [topic]
  (fn [item] (str "spot/" topic ":" (clojure.string/replace item "-" "_"))))

(defn exmo-query
  "Prepare Exmo websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "subscribe"
    :topics (map (exmo-topic "trades") trades)
  }))


(defn exmo-gather
  "Gather from Exmo"
  [conn trades]
  (let [ws @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    (println "Connected!")
    (s/put-all! ws [(exmo-query trades)])
    (while true (let [chunk (json/read-str @(s/take! ws))]
      (if (= "update" (get chunk "event"))
        (let [
          ;topic "spot/trades"
          [topic pair] (str/split (get chunk "topic") #":")
          data (get chunk "data")
          ]
          (case topic
            "spot/trades" (put-trades! conn "Exmo" pair (map exmo-trade data))))
    )))))

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)))

(defn create-market-tables
  "Create market tables"
  [conn market pairs]
  (let [queries (create-market-tables-queries market pairs)]
    (println "Executing create:")
    (print-vec queries)
    (ch/exec-vec! conn queries)
  ))

(defn main
  "Entry point"
  [db-url markets]
  (let [
    conn (ch/connect db-url)
    stmt (.createStatement conn)
    ]
    (doseq [[market, pairs] markets]
      (create-market-tables conn market pairs))
    (exmo-gather conn (get markets "Exmo")
  )))

(defn start
  "Start with params"
  []
  (main ch-url {
    "Exmo" [
      "BTC-USD" "ETH-USD" "XRP-USD" "BCH-USD" "EOS-USD" "DASH-USD" "WAVES-USD"
      "ADA-USD" "LTC-USD" "BTG-USD" "ATOM-USD" "NEO-USD" "ETC-USD" "XMR-USD"
      "ZEC-USD" "TRX-USD"
    ]}))

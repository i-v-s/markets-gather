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
    [clojure.core.async :as a]
    [clojure.data.json :as json]
    [gather.ch :as ch]
    [gather.common :as c]))

(def trade-rec {
  :id "Int32"
  :time "DateTime"
  :buy "UInt8"
  :price "Float64"
  :coin "Float32"
  :base "Float32"
  })

(def ch-url "jdbc:clickhouse://127.0.0.1:9000")

(defn create-market-tables-queries
  "Get queries for market tables creation"
  [market pairs]
  (concat
    ["CREATE DATABASE IF NOT EXISTS fx"]
    (map (fn [pair]
      (ch/create-table-query
        (str "fx." (c/lower market) "_" (c/lower pair) "_trades") trade-rec
      :engine "ReplacingMergeTree()" :partition-by "toYYYYMM(time)" :order-by ["id"]))
      pairs)
  ))

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


(defn put-trades
  "Put trades record into Clickhouse"
  [stmt trades]
  )


(defn exmo-gather
  "Gather from Exmo"
  [conn trades]
  (let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    (print "Connected!")
    (s/put-all! conn [(exmo-query trades)])
    (take 10 (for [x (range 10)]
      (println (str "\nChunk:\n" @(s/take! conn)))
    ))))
    ;(while true (print @(s/take! conn)))))

;(let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
;  (print "Connected!")
;  (s/put-all! conn ["{\"id\":1,\"method\":\"subscribe\",\"topics\":[\"spot/trades:BTC_USD\",\"spot/ticker:LTC_USD\"]}"])
;  (while true (print @(s/take! conn)))
;)

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)))

(defn create-market-tables
  "Create market tables"
  [conn market pairs]
  (let [queries (create-market-tables-queries "Exmo" ["BTC-USD", "ETH-USD"])]
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
  (main ch-url {"Exmo" ["BTC-USD" "ETH-USD"]}))

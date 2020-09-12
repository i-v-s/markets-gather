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
    [gather.ch :as ch]))

(def trade-rec {
  :id "Int32"
  :time "DateTime"
  :buy "UInt8"
  :price "Float64"
  :coin "Float32"
  :base "Float32"
  })

(def ch-url "jdbc:clickhouse://127.0.0.1:9000")

(defn lower
  "Convert name to lowercase and '-' to '_'"
  [name]
  (clojure.string/lower-case (clojure.string/replace name "-" "_")))

(defn create-market-tables-queries
  "Get queries for market tables creation"
  [market pairs]
  (concat
    ["CREATE DATABASE IF NOT EXISTS fx"]
    (map (fn [pair]
      (ch/create-table-query
        (str "fx." (lower market) "_" (lower pair) "_trades") trade-rec
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

(defn exmo-gather
  "Gather from Exmo"
  [trades]
  (let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    (print "Connected!")
    (s/put-all! conn [(exmo-query trades)])
    (take 10 (for [x (range 10)]
      (print (str "\nChunk:\n" @(s/take! conn)))
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

(defn main
  "Entry point"
  [db-url]
  (let [
    stmt (ch/connect-st db-url)
    create-queries (create-market-tables-queries "Exmo" ["BTC-USD", "ETH-USD"])
    ]
    (println "Executing create:")
    (print-vec create-queries)
    (ch/exec-vec! stmt create-queries)
  ))

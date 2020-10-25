(ns gather.core
  (:require
    [compojure.core :as compojure :refer [GET]]
    [ring.middleware.params :as params]
    [compojure.route :as route]
    [byte-streams :as bs]
    [manifold.deferred :as d]
    [manifold.bus :as bus]
    [clojure.core.async :as a]
    [gather.ch :as ch]
    [gather.drop :as drop]
    [gather.common :as c]
    [gather.exmo :as exmo]
    [gather.binance :as binance]))

(def trade-rec {
  :id "Int32 CODEC(Delta, LZ4)"
  :time "DateTime"
  :buy "UInt8"
  :price "Float64 CODEC(Gorilla)"
  :coin "Float32"
  :base "Float32"
  })

(def depth-rec {
  :id "Int64 CODEC(Delta, LZ4)"
  :time "DateTime"
  :price "Float64 CODEC(Gorilla)"
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
    (map (fn [pair]
      (ch/create-table-query
        (c/depths-table-name market pair true) depth-rec
      :engine "ReplacingMergeTree()" :partition-by "toYYYYMM(time)" :order-by ["id" "price"]))
      pairs)
    (map (fn [pair]
      (ch/create-table-query
        (c/depths-table-name market pair false) depth-rec
      :engine "ReplacingMergeTree()" :partition-by "toYYYYMM(time)" :order-by ["id" "price"]))
      pairs)
  ))

(defn put-trades!
  "Put trades record into Clickhouse"
  [conn market pair trades]
  (print (get market 0)) (flush)
  ;(println market pair "trades" (count trades))
  (ch/insert-many! conn
    (str
      "INSERT INTO " (c/trades-table-name market pair)
      "(id, time, buy, price, coin, base) VALUES (?, ?, ?, ?, ?, ?)")
    trades
  ))

(defn put-depth-query
  [market pair buy?]
  (str
    "INSERT INTO " (c/depths-table-name market pair buy?)
    "(id, time, price, base) VALUES (?, ?, ?, ?)"))

(defn put-depth!
  "Put depth records into Clickhouse"
  [conn market pair [buy sell]]
  ;(print (get market 0)) (flush)
  (println market pair "buy" (count buy) "sell" (count sell))
  (ch/insert-many! conn (put-depth-query market pair true) buy)
  (ch/insert-many! conn (put-depth-query market pair false) sell)
  )

(def ch-url "jdbc:clickhouse://127.0.0.1:9000")

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)))

(defn create-market-tables
  "Create market tables"
  [conn market pairs]
  (let [queries (create-market-tables-queries market pairs)]
    (ch/exec-vec! conn queries)
  ))

(def gather-map {
  "Exmo" exmo/gather
  "Binance" binance/gather
  })

(defn main
  "Entry point"
  [db-url markets]
  (let [
    conn (ch/connect db-url)
    ]
    (doseq [[market pairs] markets]
      (create-market-tables conn market pairs))
    (doseq [[market pairs] markets]
      (c/forever-loop market
        (fn [] ((get gather-map market) (ch/connect db-url) pairs put-trades! put-depth!))))
    (loop [] (Thread/sleep 5000) (recur))))

(defn -main
  "Start with params"
  [module & args]
  (case module
    "gather" (main ch-url {
      "Binance" [
        "BTC-USDT" "ETH-USDT" "BNB-USDT" "DOT-USDT"]
      "Exmo" [
        "BTC-USD" "ETH-USD" "XRP-USD" "BCH-USD" "EOS-USD" "DASH-USD" "WAVES-USD"
        "ADA-USD" "LTC-USD" "BTG-USD" "ATOM-USD" "NEO-USD" "ETC-USD" "XMR-USD"
        "ZEC-USD" "TRX-USD"]
      })
    "gather.drop" (drop/-main ch-url args)
  ))

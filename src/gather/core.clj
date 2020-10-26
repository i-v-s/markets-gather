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

(def trade-rec [{
    :id "Int32 CODEC(Delta, LZ4)"
    :time "DateTime"
    :buy "UInt8"
    :price "Float64 CODEC(Gorilla)"
    :coin "Float32"
    :base "Float32"
  }
  :engine "ReplacingMergeTree()"
  :partition-by "toYYYYMM(time)"
  :order-by ["id"]
  ])

(def depth-rec [{
    :id "Int64 CODEC(Delta, LZ4)"
    :time "DateTime"
    :price "Float64 CODEC(Gorilla)"
    :base "Float32"
  }
  :engine "ReplacingMergeTree()"
  :partition-by "toYYYYMM(time)"
  :order-by ["id" "price"]
  ])

(def table-types {
  :t trade-rec
  :b depth-rec
  :s depth-rec})

(defn create-market-tables-queries
  "Get queries for market tables creation"
  [market pairs]
  (concat ["CREATE DATABASE IF NOT EXISTS fx"]
    (for [pair pairs [type rec] table-types]
      (apply ch/create-table-query (c/get-table-name market pair type) rec))))

(defn market-insert-query
  [market pair type]
  (ch/insert-query
    (c/get-table-name market pair type)
    (first (type table-types))))

(defn market-inserter
  "Return function, that inserts rows to Clickhouse"
  [conn market]
  (fn [pair & args]
    (for [[type rows] (apply hash-map args)]
      (ch/insert-many! conn (market-insert-query market pair type) rows))))

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
        (fn []
          ((get gather-map market)
          pairs
          (market-inserter (ch/connect db-url) market)))))
    (loop [] (Thread/sleep 5000) (recur))))

(def pairs-list {
  "Binance" [
    "BTC-USDT" "ETH-USDT" "BNB-USDT" "DOT-USDT"]
  "Exmo" [
    "BTC-USD" "ETH-USD" "XRP-USD" "BCH-USD" "EOS-USD" "DASH-USD" "WAVES-USD"
    "ADA-USD" "LTC-USD" "BTG-USD" "ATOM-USD" "NEO-USD" "ETC-USD" "XMR-USD"
    "ZEC-USD" "TRX-USD"]
  })

(defn -main
  "Start with params"
  [module & args]
  (case module
    "gather" (main ch-url pairs-list)
    "gather.drop" (drop/-main ch-url args)
  ))

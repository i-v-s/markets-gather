(ns gather.storage
  (:require 
    [gather.ch :as ch]
    [gather.common :as c]
  ))


; Clickhouse table structures

(def trade-rec [(array-map
    :id "Int32 CODEC(Delta, LZ4)"
    :time "DateTime"
    :buy "UInt8"
    :price "Float64 CODEC(Gorilla)"
    :coin "Float32"
    :base "Float32"
  )
  :engine "ReplacingMergeTree()"
  :partition-by "toYYYYMM(time)"
  :order-by ["id"]
  ])

(def depth-rec [(array-map
    :time "DateTime"
    :price "Float64 CODEC(Gorilla)"
    :base "Float32"
  )
  :engine "ReplacingMergeTree()"
  :partition-by "toYYYYMM(time)"
  :order-by ["time" "price"]
  ])

(def price-rec [(array-map
    :time "DateTime"
    :buy "Float64 CODEC(Gorilla)"
  )
  :engine "ReplacingMergeTree()"
  :partition-by "toYYYYMM(time)"
  :order-by ["time"]
  ])

(def candle-pair-fields (array-map
  :open "Float64 CODEC(Gorilla)"
  :high "Float64 CODEC(Gorilla)"
  :low "Float64 CODEC(Gorilla)"
  :close "Float64 CODEC(Gorilla)"
  :volume "Float32"
  :q-volume "Float32"
  :trades "UInt32"
  :buy-volume "Float32"
  :buy-q-volume "Float32"
))

(def table-types {
  :t trade-rec
  :b depth-rec
  :s depth-rec
  :p price-rec})


; Clickhouse storage functions

(defn create-market-tables-queries
  "Get queries for market tables creation"
  [market pairs settings]
  (concat ["CREATE DATABASE IF NOT EXISTS fx"]
    (for [pair pairs [type rec] table-types]
      (apply ch/create-table-query (c/get-table-name market pair type) (conj rec :settings settings)))))

(defn create-market-tables!
  "Create market tables"
  [conn market pairs]
  (ch/exec-vec! conn (create-market-tables-queries market pairs ["storage_policy = 'ssd_to_hdd'"])))      

(defn market-insert-query
  [market pair type]
  (ch/insert-query
    (c/get-table-name market pair type)
    (first (type table-types))))


; Memory storage functions

(defn make-buffer
  "Create empty memory buffer in form '(count, rows)"
  [] (atom (list 0 [])))

(defn prepare-buffers
  "Prepare empty write buffers in form {market {[\"pair\" :tp] buffer}}"
  [markets]
  (into {} (for [[market pairs] markets]
    [market (into {} (for [pair pairs tp [:t :s :b]]
      [(list pair tp) (make-buffer)]))])))

(defn push-buf!
  "Push rows in atom with form '(count, rows)"
  [item rows]
  (swap! item (fn [[c v]]
    (list
      (+ c (count rows))
      (into v rows)
    ))))

(defn pop-buf!
  "Pop rows only from atom with form '(count, rows)"
  [item] (-> (swap-vals! item #(list (first %) [])) first second))

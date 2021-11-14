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
  [db market pairs settings]
  (concat [(str "CREATE DATABASE IF NOT EXISTS " db) (str "USE " db)]
    (for [pair pairs [type rec] table-types]
      (apply ch/create-table-query (c/get-table-name market pair type) (conj rec :settings settings)))))

(defn create-market-tables!
  "Create market tables"
  [{db :db url :url policy :storage-policy} {market :name {pairs :pairs} :raw}]
  ;(println
  (ch/exec-vec!
    (ch/connect url)
    (create-market-tables-queries 
      db market pairs
      (if policy [(str "storage_policy = '" policy "'")] []))
   ))

(defn market-insert-query
  [market pair type]
  (ch/insert-query
   (c/get-table-name market pair type)
   (first (type table-types))))

(defn market-insert-query
  [table type]
  (ch/insert-query
   table
   (first (type table-types))))


; Memory storage functions

(defprotocol Writable
  "A protocol that abstracts writable object"
  (push! [this rows] "Write rows to object")
  (writed-count [this] "Returns writed count"))

(deftype WriteCache [buffer table]
  Writable
  (push! [this rows]
    (swap! (.buffer this)
           (fn [[c v]]
             (list
              (+ c (count rows))
              (into v rows)))))
  (writed-count [this]
    (-> (.buffer this) deref first)))

(defn make-buffer
  "Create empty memory buffer in form '(count, rows)"
  [] (atom (list 0 [])))

(defn make-raw-buffers
  "Prepare empty write buffers in form {\"name\" [buffer table]}. \"name\" may be pair or candle interval"
  [market-name raw-pairs tp]
  (into {} (for [pair raw-pairs]
             [pair (WriteCache. (make-buffer)
                                (c/get-table-name market-name pair tp))])))

(defn push-raw! [bufs pair rows]
  (let [{item pair} bufs]
    (if item
      (push! item rows)
      (throw (Exception. (str "(push-raw!): Unknown pair " pair))))))

(defn pop-buf!
  "Pop rows only from atom with form '(count, rows)"
  [item] (-> (swap-vals! item #(list (first %) [])) first second))

(defn insert-from-raw-buffers!
  "Try to write data from raw buffers into Clickhouse and then clear them"
  [markets conn]
  (doseq [{raw :raw market-name :name} markets
          tp [:t :b :s]
          [pair [buf table]] (tp raw)
          :let [rows (pop-buf! buf)]
          :when (not-empty rows)]
    (try
      (ch/insert-many! conn (market-insert-query table tp) rows)
      (catch Exception e
        (println "\nException on insert" market-name pair tp "- repushing...")
        (swap! buf (fn [[c v]] (list c (into rows v))))
        (throw e)))))

(defn print-buffers
  "Prints statistics on buffers"
  [markets]
  (do
    (->>
      (for [{market-name :name raw :raw} markets]
        (str market-name " " (c/comma-join
          (for [tp [:t :b :s]]
                          (str (sum (comp deref first ) (map (tp raw))) (name tp))))))
     c/comma-join
     (str "\r")
     print) (flush)))

(defprotocol Market
  "A protocol that abstracts exchange interactions"
  (get-all-pairs [this] "Return all pairs for current market")
  (gather-ws-loop! [this verbose] "Gather raw data via websockets")
)

(defrecord RawData [pairs t b s])
(defrecord CandlesData [pairs intervals])

(defn make-raw-data
  "Prepare empty write buffers in form {market {[\"name\" :tp] buffer}}. \"name\" may be pair or candle interval"
  [market-name raw-pairs]
  (RawData. raw-pairs 
            (make-raw-buffers market-name raw-pairs :t)
            (make-raw-buffers market-name raw-pairs :b)
            (make-raw-buffers market-name raw-pairs :s)))

;(defn market-inserter
;  "Return function, that inserts rows to Clickhouse"
;  [market buffers]
;  (fn put! [pair & args]
;    (doseq [[tp rows] (apply hash-map args)]
;      (assert (keyword? tp))
;      (sg/push-buf! (get buffers (list pair tp)) rows))))


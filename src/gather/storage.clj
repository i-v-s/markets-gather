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

(def candle-rec [(array-map
                 :time "DateTime"
                 )
                :engine "ReplacingMergeTree()"
                :order-by ["time"]])

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

(def raw-table-types {
  :t trade-rec
  :b depth-rec
  :s depth-rec
  :p price-rec})


; Clickhouse storage functions

(defn construct-candle-rec
  [assets]
  (ch/conj-fields
   candle-rec
   (for [asset assets
         [field decl] candle-pair-fields]
     [(->> field
           name
           (str asset "_")
           c/lower
           keyword)
      decl])))

(defn construct-candle-recs
  [candles assets & {:keys [settings] :or {settings []}}]
  (into {}
        (for [candle candles]
          [candle
           (into []
                 (concat
                  (construct-candle-rec assets)
                  (if (< (c/intervals-map candle) (c/intervals-map :1d))
                    [:partition-by "toYear(time)"] [])
                  settings))])))

(defn get-candle-table-name
  [market quote candle]
  (c/get-table-name market (str quote "_" (c/candle-name candle)) :c))

(defn ensure-tables!
  "Create market tables"
  [{db :db url :url policy :storage-policy}
   {market :name
    {raw-pairs :pairs} :raw
    {candle-pairs :pairs candles :intervals} :candles}]
  (let [conn (ch/connect url)
        settings (if policy [(str "storage_policy = '" policy "'")] [])]
    ;(.setClientInfo conn "max_query_size" "1000000")
    (ch/create-db! conn db)
    (ch/use! conn db)
    (->>
     [(for [pair raw-pairs [tp rec] raw-table-types]
        [(c/get-table-name market pair tp)
         (conj rec :settings settings)])
      (for [[quote assets] candle-pairs
            [candle rec] (construct-candle-recs candles assets)]
        [(get-candle-table-name market quote candle)
         (conj rec :settings settings)])]
     (apply concat)
     (into {})
     (ch/ensure-tables! conn))))

(defn market-insert-query
  ([market pair type]
   (ch/insert-query
    (c/get-table-name market pair type)
    (first (type raw-table-types))))
  ([table type]
   (ch/insert-query
    table
    (first (type raw-table-types)))))


; Memory storage functions

(defprotocol Accumulator
  "A protocol that abstracts writable object"
  (push! [this rows] "Write rows to back")
  (repush! [this rows] "Write rows to start, without change writed count")
  (writed-count [this] "Returns writed count")
  (pop-all! [this] "Pop rows only from atom with form '(count, rows)"))

(deftype WriteCache [buffer table]
  Accumulator
  (push! [this rows]
    (swap! (.buffer this)
           (fn [[c v]]
             (list
              (+ c (count rows))
              (into v rows)))))
  (repush! [this rows]
    (swap! (.buffer this)
           (fn [[c v]]
             (list
              c
              (into rows v)))))
  (writed-count [this]
    (-> (.buffer this) deref first))
  (pop-all! [this]
    (-> (swap-vals! (.buffer this) #(list (first %) [])) first second)))

(defn make-buffer
  "Create empty memory buffer in form '(count, rows)"
  [] (atom (list 0 [])))

(defn make-raw-buffers
  "Prepare empty write buffers in form {\"name\" [buffer table]}. \"name\" may be pair or candle interval"
  [market raw-pairs tp]
  (into {} (for [pair raw-pairs]
             [pair (WriteCache. (make-buffer)
                                (c/get-table-name market pair tp))])))

(defn push-raw! [bufs pair rows]
  (let [{item pair} bufs]
    (if item
      (push! item rows)
      (throw (Exception. (str "(push-raw!): Unknown pair " pair))))))

(defn insert-from-raw-buffers!
  "Try to write data from raw buffers into Clickhouse and then clear them"
  [markets conn]
  (doseq [{raw :raw market-name :name} markets
          tp [:t :b :s]
          [pair cache] (tp raw)
          :let [rows (pop-all! cache)]
          :when (not-empty rows)]
    (try
      (ch/insert-many! conn (market-insert-query (.table cache) tp) rows)
      (catch Exception e
        (println "\nException on insert" market-name pair tp "- repushing...")
        (repush! cache rows)
        (throw e)))))

(defprotocol Considerable
  (stats [this] "Return stats string of object"))

(defrecord RawData [pairs t b s]
  Considerable
  (stats [this]
    (c/comma-join
     (for [tp [:t :b :s]]
       (str (->> this
                 tp
                 (map (comp writed-count second))
                 (reduce +))
            (name tp))))))

(defrecord CandlesData [pairs intervals])

(defn make-raw-data
  "Prepare empty write buffers in form {market {[\"name\" :tp] buffer}}. \"name\" may be pair or candle interval"
  [market-name raw-pairs]
  (RawData. raw-pairs
            (make-raw-buffers market-name raw-pairs :t)
            (make-raw-buffers market-name raw-pairs :b)
            (make-raw-buffers market-name raw-pairs :s)))

(defn print-buffers
  "Prints statistics on buffers"
  [markets]
  (->>
   (for [{market-name :name raw :raw} markets]
     (str market-name " " (stats raw)))
   c/comma-join
   (str "\r")
   print)
  (flush))

(defprotocol Market
  "A protocol that abstracts exchange interactions"
  (get-all-pairs [this] "Return all pairs for current market")
  (gather-ws-loop! [this verbose] "Gather raw data via websockets")
  (get-candles [this pair interval start end])
)


; Candle functions

(deftype CandleState [data])

(defn make-candle-state
  []
  (CandleState. (atom [[] nil])))

(defn get-candles-batch
  [market quote assets tf & {:keys [start starts] :or {starts {}}}]
  (when (and (not start) (not= tf :1M))
    (throw (Exception. "get-candles-batch: Only month candles allowed without start time specified")))
  (let [{limit :candles-limit} market
        end (if start (+ start (* limit (tf c/intervals-map))) nil)]
    (for [asset assets
          :let
          [pair (str asset "-" quote)
           start' (c/ts-max start (starts asset))]]
      (get-candles market pair tf start' end))))

(defn candle-batch-to-rows
  "Returns rows in format '(long time, assets, data)"
  [assets batch]
  (let [maps (mapv (partial into {}) batch)
        times (sort (c/set-of-keys maps))]
    (for [time times :let [get-time #(% time)]]
      (cons time
       (->> maps
            (map get-time)
            (map list assets)
            (filter second)
            (apply map list)
            )))))

(defn group-candle-rows
  [rows]
  (map
   (juxt first
         (comp
          (partial
           map
           (fn [[time _ items]]
             (cons
              (java.sql.Timestamp. time)
              (apply concat items))))
          last))
   (group-by second rows)))

(defn insert-candle-rows!
  [conn table assets rows]
  (ch/insert-many!
   conn
   (ch/insert-query
    table
    (first (construct-candle-rec assets)))
   rows))

(defn grab-candles!
  "Return last ts"
  [conn market quote tf]
  (let [assets (-> market :candles :pairs (get quote))
        table (get-candle-table-name (:name market) quote tf)
        current (c/dec-ts (c/now-ts) tf)
        rows (->> (get-candles-batch market quote assets tf)
                  (candle-batch-to-rows assets)
                  (filter (comp (partial > current) first)))
        last-ts (-> rows last first)]
    (->> rows
         (group-candle-rows)
         (map (partial apply insert-candle-rows! conn table))
         doall)
    last-ts))

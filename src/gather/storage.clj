(ns gather.storage
  (:require
   [clojure.tools.logging :refer [trace debug info warn error]]
   [clojure.string :as str]
   [exch.utils     :as xu]
   [house.ch       :as ch]
   [gather.common  :as c]))

(import java.sql.Timestamp)


; Clickhouse table structures

(def trade-rec [(array-map
    :id "Int32 CODEC(Delta(4), LZ4)"
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

(def candle-mapping {:time         :open-ts
                     :open         :open
                     :high         :high
                     :low          :low
                     :close        :close
                     :volume       :volume
                     :q-volume     :quote
                     :trades       :trades
                     :buy-volume   :buy-volume
                     :buy-q-volume :buy-quote})

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
  [market-name quote tf]
  (c/get-table-name market-name (str quote "_" (c/candle-name tf)) :c))

(defn ensure-tables!
  "Create market tables"
  [{url :url policy :storage-policy}
   {market :name
    {raw-pairs :pairs} :raw
    {candle-pairs :pairs candles :intervals} :candles}]
  (let [conn (ch/connect url)
        settings (if policy [(str "storage_policy = '" policy "'")] [])]
    (->> [(for [pair raw-pairs [tp rec] raw-table-types]
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
        (error "Exception on insert" market-name pair tp "- repushing...")
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
  (assert (not-empty raw-pairs) (str "Empty raw-pairs for market " market-name))
  (RawData. raw-pairs
            (make-raw-buffers market-name raw-pairs :t)
            (make-raw-buffers market-name raw-pairs :b)
            (make-raw-buffers market-name raw-pairs :s)))

(defn print-buffers
  "Prints statistics on buffers"
  [markets]
  (->>
   (for [{market-name :name raw :raw} markets
         :when raw]
     (str market-name " " (stats raw)))
   c/comma-join
   (str "\r")
   print)
  (flush))


; Candle functions

(defn get-candle-starts
  [conn market quote tf]
  (let [assets (-> market :candles :pairs (get quote))
        table (get-candle-table-name (:name market) quote tf)]
    (->> assets
         (map
          #(str
            "(SELECT MIN(time) FROM " table
            " WHERE " (str/lower-case %) "_open != 0) AS " %))
         c/comma-join
         (str "SELECT ")
         (ch/exec! conn)
         ch/fetch-one
         (map (fn [k v] [k (.getTime v)]) assets)
         (filter (comp pos? second)))))

(defn get-last-candle
  [conn table]
  (->> table
       (str "SELECT MAX(time) FROM ")
       (ch/exec! conn)
       ch/fetch-one
       first
       .getTime
       ))

(defn get-next-candle
  [conn table tf]
  (let [lc (get-last-candle conn table)]
    (if (pos? lc)
      (c/inc-ts lc tf)
      nil)))

(defn candle-market-fields
  [market]
  (->> (concat (-> candle-rec first keys) (-> candle-pair-fields keys))
       (map candle-mapping)
       (filterv (xu/get-rec market :candle))))

(defn get-candles
  [market fields tf quote asset start end]
  (let [pair (str asset "-" quote)
        candles
        (c/with-retry 5
          (xu/get-candles market nil fields tf pair start end))]
    (if (empty? candles)
      (warn "get-candles-batch: no data received" (:name market) pair tf (c/format-interval tf start end))
      (trace "get-candles-batch requested:" (:name market) pair tf (c/format-interval tf start end)
             "received:" (c/format-interval tf (ffirst candles) (first (last candles))) "count:" (count candles)))
    candles))

(defn get-candles-batch
  [get-candles assets & {:keys [start starts end] :or {starts {}}}]
  (for [asset assets
        :let [start' (c/ts-max start (starts asset))]]
    (if (or (nil? start') (< start' end))
      (get-candles asset start' end)
      [])))

(defn candle-batch-to-rows
  "Returns rows in format '(long time, assets, data)"
  [zeros until batch]
  (let [maps  (mapv (comp (partial into {}) (partial map (juxt first rest))) batch)]
    (for [time  (sort (c/set-of-keys maps))
          :when (> until time)]
      (->> maps
           (map #(or (% time) zeros))
           (apply concat)
           (cons (Timestamp. time))))))

(defn grab-candles!
  [get-candles zeros assets until & args]
  (->> (apply get-candles-batch get-candles assets args)
       (candle-batch-to-rows zeros until)))

(defn prepare
  [conn table assets]
  (let [cols (-> assets construct-candle-rec first)]
    (debug "Preparing insert statement for" table "cols:" (count cols))
    (->> cols (ch/insert-query table) (ch/prepare conn))))

(defn insert-candle-rows!
  [p-st table assets rows]
  (trace "Inserting into" table (count rows) "row(s);"
         "interval" (c/format-interval :1m (ffirst rows) (first (last rows)))
         "cols:" (-> rows first count)
         "assets:" (count assets) assets)
  (ch/insert-many! p-st rows))

(defn grab-all-candles!
  [conn market]
  (doseq [:let [{market-name :name {pairs :pairs tfs :intervals} :candles} market
                market-fields (candle-market-fields market)]
          quote (keys pairs)
          :let [assets (-> market :candles :pairs (get quote))
                starts (atom {})]
          tf tfs]
    (let [table         (get-candle-table-name market-name quote tf)
          get-candles   (partial get-candles market market-fields tf quote)
          grab-candles! (partial grab-candles! get-candles (-> market-fields count dec (repeat 0) vec))
          start         (atom (or
                               (get-next-candle conn table tf)
                               (if (not-empty @starts) (apply min (vals @starts)) nil)))
          p-st          (atom nil)
          current       (c/dec-ts (c/now-ts) tf)]

      (while (or (nil? @start) (< @start current))
        (debug "Grabbing candles from" market-name quote tf (if @start (c/ts-str @start) "*"))
        (let [end (and @start
                       (min
                        (c/inc-ts @start tf :mul (-> market :candles-limit dec))
                        (c/now-ts)))
              rows (grab-candles! assets current :start @start :starts @starts :end end)]
          (if (not-empty rows)
            (do
              (c/init-nil! p-st (prepare conn table assets))
              (insert-candle-rows! @p-st table assets rows))
            (warn "No rows to insert from" market-name quote tf (if @start (c/ts-str @start) "*")))
          (reset! start (if end (c/inc-ts end tf) current))))
      (info "Completed candles:" market-name quote tf))

    (when (not= tf (last tfs))
      (reset! starts (into {} (get-candle-starts conn market quote tf))))))

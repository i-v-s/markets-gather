(ns gather.core
  (:require
    [gather.config :as config]
    [gather.ch :as ch]
    [gather.storage :as storage]
    [gather.drop :as drop]
    [gather.backup :as backup]
    [gather.restore :as restore]
    [gather.common :as c]
    [gather.exmo :as exmo]
    [gather.binance :as binance]
  ))

(defn market-inserter
  "Return function, that inserts rows to Clickhouse"
  [market buffers]
  (fn put! [pair & args]
    (doseq [[tp rows] (apply hash-map args)]
      (assert (keyword? tp))
      (storage/push-buf! (get buffers (list pair tp)) rows))
    ))

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)
  ))

(def gather-map {
  :exmo {
    :ws exmo/gather-ws
    :get-all-pairs exmo/get-all-pairs}
  :binance {
    :ws binance/gather-ws
    :get-all-pairs binance/get-all-pairs}
  })

(defn raw-insert-loop!
  "Worker, that periodicaly inserts rows from raw buffers into Clickhouse"
  [buffers {db-url :url db :db}]
  (let [conn (ch/connect db-url)]
    (println (str "\n" (new java.util.Date) ": Started Core"))
    (loop []
      (Thread/sleep 2000)
      (storage/insert-from-raw-buffers! buffers conn db)
      (storage/print-buffers buffers)
      (recur))
  ))

(defn main
  "Entry point"
  [& [config-file-name]]
  (let [
      {db-cfg :clickhouse markets :markets} (config/load-config config-file-name)
      buffers (storage/make-raw-buffers markets)
    ]
    (run! (partial apply storage/create-market-tables! db-cfg) markets)
    (doseq [
        [market {pairs :raw-pairs}] markets
        :let [
          funcs (market gather-map)
          all-pairs ((:get-all-pairs funcs))
          market-buf (market buffers)
          market-name (c/capitalize-key market)
        ]]
      (c/forever-loop #(do
        (println (str "\n" (new java.util.Date) " Starting " market-name " WS"))
        ((:ws funcs) pairs (market-inserter market market-buf))
      ) :title (str market-name " WS"))
      ;(c/forever-loop #(do
      ;  (println (str "\n" (new java.util.Date) " Starting " market-name " REST"))
      ;) :title (str market-name " REST")
      )
    (c/try-loop (partial raw-insert-loop! buffers db-cfg) :title "Core" :delay 10000)
  ))

(defn -main
  "Start with params"
  [module & args]
  (case module
    "gather" (-> args (c/parse-options :config :help) :options :config main)
    "gather.drop" (drop/-main args)
    "gather.backup" (apply backup/-main args)
    "gather.restore" (apply restore/-main args)
  ))

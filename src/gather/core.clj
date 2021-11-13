(ns gather.core
  (:require
    [gather.config :as cfg]
    [gather.ch :as ch]
    [gather.storage :as sg]
    [gather.drop :as drop]
    [gather.backup :as backup]
    [gather.restore :as restore]
    [gather.common :as c]
  ))

(defn market-inserter
  "Return function, that inserts rows to Clickhouse"
  [market buffers]
  (fn put! [pair & args]
    (doseq [[tp rows] (apply hash-map args)]
      (assert (keyword? tp))
      (sg/push-buf! (get buffers (list pair tp)) rows))
    ))

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)
  ))

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

(defn candles-insert-loop!
  [market candles pairs db-cfg get-candles]
  (println candles)
  (println pairs))

(defn main
  "Entry point"
  [& [config-file-name]]
  (let [{db-cfg :clickhouse markets :markets} (cfg/load-config config-file-name)]
    (run! (partial apply sg/create-market-tables! db-cfg) markets)
    (doseq [market markets]
      (c/forever-loop #(do
        (println (str "\n" (new java.util.Date) " Starting " (:name market) " WS"))
        ((:ws funcs) pairs (market-inserter market market-buf))
      ) :title (str market-name " WS"))
      ;(c/forever-loop #(do
      ;  (println (str "\n" (new java.util.Date) " Starting " market-name " REST"))
      ;) :title (str market-name " REST")
      (candles-insert-loop! market candles all-pairs db-cfg (:get-candles funcs))
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

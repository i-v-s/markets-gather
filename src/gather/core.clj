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

(defn raw-insert-loop!
  "Worker, that periodicaly inserts rows from raw buffers into Clickhouse"
  [markets {db-url :url db :db}]
  (let [conn (ch/connect db-url)]
    (ch/use! conn db)
    (println (str "\n" (new java.util.Date) ": Started Core"))
    (loop []
      (Thread/sleep 2000)
      (sg/insert-from-raw-buffers! markets conn)
      (sg/print-buffers markets)
      (recur))))

(defn candles-insert-loop!
  [market db-cfg]
  ;(println market)
  (println "Candles-insert-loop"))

(defn main
  "Entry point"
  [& [config-file-name]]
  (let [{db-cfg :clickhouse markets :markets} (cfg/load-config config-file-name)]
    (run! (partial sg/create-market-tables! db-cfg) markets)
    (doseq [market markets]
      (c/forever-loop #(do
        (println (str "\n" (new java.util.Date) " Starting " (:name market) " WS"))
        (sg/gather-ws-loop! market :info))
      :title (str (:name market) " WS"))
      ;(c/forever-loop #(do
      ;  (println (str "\n" (new java.util.Date) " Starting " market-name " REST"))
      ;) :title (str market-name " REST")
      (candles-insert-loop! market db-cfg)
      )
    (c/try-loop (partial raw-insert-loop! markets db-cfg) :title "Core" :delay 10000)
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

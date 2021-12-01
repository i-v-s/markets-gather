(ns gather.core
  (:require
   [clojure.core.async :as a]
   [gather.config :as cfg]
   [gather.ch :as ch]
   [gather.storage :as sg]
   [gather.drop :as drop]
   [gather.backup :as backup]
   [gather.restore :as restore]
   [gather.common :as c]
   [gather.check :as ck]))

(defn raw-insert-loop!
  "Worker, that periodicaly inserts rows from raw buffers into Clickhouse"
  [markets {db-url :url}]
  (let [conn (ch/connect db-url)]
    (println (str "\n" (c/now) " Started Core"))
    (loop []
      (Thread/sleep 2000)
      (sg/insert-from-raw-buffers! markets conn)
      (sg/print-buffers markets)
      (recur))))

(defn candles-insert-loop!
  [market {db-url :url}]
  (sg/grab-all-candles!
   (ch/connect db-url)
   market)
  (println "\nCandle grabbing completed"))

(defn main
  "Entry point"
  [& [config-file-name]]
  (let [{db-cfg :clickhouse markets :markets} (cfg/load-config config-file-name)]
    (ch/create-db! (:url db-cfg))
    (run! (partial sg/ensure-tables! db-cfg) markets)
    (doseq [market markets]
      (c/forever-loop #(do
        (println (str "\n" (c/now) " Starting " (:name market) " WS"))
        (sg/gather-ws-loop! market :info))
      :title (str (:name market) " WS"))
      (a/thread
       (candles-insert-loop! market db-cfg))
      )
    (c/try-loop (partial raw-insert-loop! markets db-cfg) :title "Core" :delay 10000)
  ))

(defn -main
  "Start with params"
  [module & args]
  (case module
    "gather" (-> args (c/parse-options :config :help) :options :config main)
    "check" (let [{{url :db-url config :config} :options args :arguments} (c/parse-options args :config :db-url :help)]
             (ck/-main (or url (-> config cfg/load-json :clickhouse :url)) args))
    "drop" (let [{{url :db-url config :config} :options args :arguments} (c/parse-options args :config :db-url :help)]
             (drop/-main (or url (-> config cfg/load-json :clickhouse :url)) args))
    "backup" (apply backup/-main args)
    "restore" (apply restore/-main args)))

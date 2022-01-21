(ns gather.core
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as a]
   [clojure.tools.logging :refer [info error]]
   [gather.config :as cfg]
   [gather.ch :as ch]
   [gather.storage :as sg]
   [gather.drop :as drop]
   [gather.backup :as backup]
   [gather.restore :as restore]
   [gather.common :as c]
   [gather.check :as ck]
   [gather.binance :as binance]))

(defn raw-insert-loop!
  "Worker, that periodicaly inserts rows from raw buffers into Clickhouse"
  [markets {db-url :url}]
  (let [conn (ch/connect db-url)]
    (info "Started Core")
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
  (info "Candle grabbing completed for market" (:name market)))

(defn main
  "Entry point"
  [& [config-file-name]]
  (info "Gather started")
  (let [{db-cfg :clickhouse markets :markets} (cfg/load-config config-file-name)]
    (ch/create-db! (:url db-cfg))
    (run! (partial sg/ensure-tables! db-cfg) markets)
    (doseq [market markets]
      (c/forever-loop
       #(do
          (info "Starting" (:name market) "WS")
          (sg/gather-ws-loop! market :info))
       :title (str (:name market) " WS"))
      (a/thread
        (try
          (candles-insert-loop! market db-cfg)
          (catch Exception e
            (error e "Market" (:name market) "- exception in candles-insert-loop!")))))
    (c/try-loop (partial raw-insert-loop! markets db-cfg) :title "Core" :delay 10000)))

(defn -main
  "Start with params"
  [module & args]
  (case module
    "gather" (-> args (c/parse-options :config :help) :options :config main)
    "check" (let [{{url :db-url config :config verbosity :verbosity} :options args :arguments} (c/parse-options args :config :db-url :help :verbosity)]
              (ck/-main (or url (-> config cfg/load-json :clickhouse :url)) args verbosity))
    "drop" (let [{{url :db-url config :config} :options args :arguments} (c/parse-options args :config :db-url :help)]
             (drop/-main (or url (-> config cfg/load-json :clickhouse :url)) args))
    "backup" (apply backup/-main args)
    "restore" (apply restore/-main args)
    "spreads" (let [ps (sg/get-all-pairs (binance/create))]
                (doseq [[p s] (binance/get-current-spreads ps)]
                  (println p s)
                  (with-open [w (io/writer "spreads.txt" :append true)]
                    (.write w (str (pr-str [p s]) "\n"))))
                (println "Completed"))))

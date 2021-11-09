(ns gather.core
  (:require
    [compojure.core :as compojure :refer [GET]]
    [ring.middleware.params :as params]
    [compojure.route :as route]
    [byte-streams :as bs]
    [manifold.deferred :as d]
    [manifold.bus :as bus]
    [clojure.core.async :as a]
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

(def ch-url "jdbc:clickhouse://127.0.0.1:9000")

(defn print-vec
  "Prints vector of strings"
  [lines]
  (dorun (map println lines)
  ))

(def gather-map {
  "Exmo" exmo/gather
  "Binance" binance/gather
  })

(defn main
  "Entry point"
  [& config-fn]
  (let [
      {db :clickhouse markets :markets} (config/load-config config-fn)
      db-url (:url db)
      buffers (storage/prepare-buffers markets)
      show! #(do (->>
        (for [[market pairs] buffers]
          (str market " " (c/comma-join
            (for [[k v] (c/map-sum pairs :k second :v (comp first deref))]
              (str v (name k))))))
        c/comma-join
        (str "\r")
        print) (flush))
      put! #(doseq [
          [market pairs] buffers
          [[pair tp] buf] pairs
          :let [rows (storage/pop-buf! buf)]
          :when (not-empty rows)
        ]
        (try
          (ch/insert-many! % (storage/market-insert-query market pair tp) rows)
          (catch Exception e
            (println "\nException on insert" market pair tp "- repushing...")
            (swap! buf (fn [[c v]](list c (into rows v))))
            (throw e))))
    ]
    (run! (partial apply storage/create-market-tables! (ch/connect db-url)) markets)
    (doseq [
      [market pairs] markets
      :let [gather! (get gather-map market) market-buf (get buffers market)]
      ]
      (c/forever-loop
        #(do
          (println (str "\n" (new java.util.Date) ": Starting " market))
          (gather! pairs (market-inserter market market-buf))
        ) :title market))
    (c/try-loop
      #(let [conn (ch/connect db-url)]
        (println (str "\n" (new java.util.Date) ": Started Core"))
        (loop []
          (Thread/sleep 2000)
          (put! conn)
          (show!)
          (recur))
      ) :title "Core" :delay 10000)
  ))

(def pairs-list {
  "Binance" [
    "BTC-USDT" "ETH-USDT" "BNB-USDT" "DOT-USDT" "CFX-USDT" "ETC-USDT" "LTC-USDT" "RVN-USDT" "EOS-USDT" "1INCH-USDT" "MATIC-USDT"
    "ZIL-USDT" "WAVES-USDT" "XRP-USDT" "TRX-USDT" "ADA-USDT" "DOGE-USDT" "CAKE-USDT" "VET-USDT" "BAKE-USDT" "DOT-USDT" "XMR-USDT"]
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
    "gather.backup" (apply backup/-main ch-url args)
    "gather.restore" (apply restore/-main ch-url args)
  ))

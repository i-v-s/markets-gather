(ns gather.core
  (:require
    [compojure.core :as compojure :refer [GET]]
    [ring.middleware.params :as params]
    [compojure.route :as route]
    [aleph.http :as http]
    [byte-streams :as bs]
    [manifold.stream :as s]
    [manifold.deferred :as d]
    [manifold.bus :as bus]
    [clojure.core.async :as a]
    [clojure.data.json :as json]
    [gather.ch :as ch]))

(def trade-rec {
  :id "Int32"
  :time "time"
  :buy "Uint8"
  :price "Float64"
  :coin "Float32"
  :base "Float32"
  })

(defn comma-join [items]
  "Join items with comma"
  (clojure.string/join ", " items))

(defn column-query
  [item]
  (let [[key desc] item]
    (str (name key) " " desc)))

(defn create-table-query
  "Return create table query"
  [name rec & {
    :keys [order-by engine partition-by]
    :or {
      engine "MergeTree()"
      partition-by "toYYYYMM(time)"
    }
    }]
  (str
    "CREATE TABLE IF NOT EXISTS " name "(\n"
    (clojure.string/join ",\n" (map column-query rec))
    "\n) ENGINE = " engine
    " ORDER BY " (comma-join order-by)
    (if partition-by (str " PARTITION BY " partition-by) "")
    ))

(defn exmo-topic
  "Convert pair to topic"
  [topic]
  (fn [item] (str "spot/" topic ":" (clojure.string/replace item "-" "_"))))

(defn exmo-query
  "Prepare Exmo websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "subscribe"
    :topics (map (exmo-topic "trades") trades)
  }))

(defn exmo-gather
  "Gather from Exmo"
  [trades]
  (let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    (print "Connected!")
    (s/put-all! conn [(exmo-query trades)])
    (take 10 (for [x (range 10)]
      (print (str "\nChunk:\n" @(s/take! conn)))
    ))))
    ;(while true (print @(s/take! conn)))))

;(let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
;  (print "Connected!")
;  (s/put-all! conn ["{\"id\":1,\"method\":\"subscribe\",\"topics\":[\"spot/trades:BTC_USD\",\"spot/ticker:LTC_USD\"]}"])
;  (while true (print @(s/take! conn)))
;)

(exmo-query ["BTC-USD", "ETH-USD"])

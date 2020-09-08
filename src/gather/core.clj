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
    [clojure.data.json :as json]))

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
    (while true (print @(s/take! conn)))))

;(let [conn @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
;  (print "Connected!")
;  (s/put-all! conn ["{\"id\":1,\"method\":\"subscribe\",\"topics\":[\"spot/trades:BTC_USD\",\"spot/ticker:LTC_USD\"]}"])
;  (while true (print @(s/take! conn)))
;)

(exmo-query ["BTC-USD", "ETH-USD"])

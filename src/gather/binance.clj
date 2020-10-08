(ns gather.binance
  (:require
    [clojure.string :as str]
    [manifold.stream :as s]
    [clojure.data.json :as json]
    [aleph.http :as http]
  ))

(defn trade-stream
  "Convert pair to trade topic name"
  [item]
  (str
    (clojure.string/lower-case (clojure.string/replace item "-" ""))
    "@trade"))

(defn ws-query
  "Prepare websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "SUBSCRIBE"
    :params (map trade-stream trades)
  }))

(defn gather
  "Gather from Binance"
  [conn trades put-trades!]
  (let [ws @(http/websocket-client (str
    "wss://stream.binance.com:9443/stream?streams="
    (clojure.string/join "/" (map trade-stream trades))
    ))]
    (println "Connected!")
    (s/put-all! ws [(ws-query trades)])
    (while true (let [chunk (json/read-str @(s/take! ws))]
      (println chunk)
;      (if (= "update" (get chunk "event"))
;        (let [
;          ;topic "spot/trades"
;          [topic pair] (str/split (get chunk "topic") #":")
;          data (get chunk "data")
;          ]
;          (case topic
;            "spot/trades" (put-trades! conn "Exmo" pair (map transform-trade data))))
    ))))

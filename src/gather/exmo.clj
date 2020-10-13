(ns gather.exmo
  (:require
    [clojure.string :as str]
    [manifold.stream :as s]
    [clojure.data.json :as json]
    [aleph.http :as http]
  ))

(defn transform-trade
  "Transform Exmo trade record to Clickhouse row"
  [r]
  [
    (get r "trade_id")
    (new java.sql.Timestamp (* 1000 (get r "date")))
    (if (= "buy" (get r "type")) 1 0)
    (Float/parseFloat (get r "price"))
    (Float/parseFloat (get r "quantity"))
    (Float/parseFloat (get r "amount"))
  ])

(defn pair-topic
  "Convert pair to topic"
  [topic]
  (fn [item] (str "spot/" topic ":" (clojure.string/replace item "-" "_"))))

(defn ws-query
  "Prepare Exmo websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "subscribe"
    :topics (map (pair-topic "trades") trades)
  }))

(defn gather
  "Gather from Exmo"
  [conn trades put-trades!]
  (let [ws @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    (println "Connected to Exmo")
    (s/put-all! ws [(ws-query trades)])
    (while true (let [chunk (json/read-str @(s/take! ws))]
      (if (= "update" (get chunk "event"))
        (let [
          ;topic "spot/trades"
          [topic pair] (str/split (get chunk "topic") #":")
          data (get chunk "data")
          ]
          (case topic
            "spot/trades" (put-trades! conn "Exmo" pair (map transform-trade data))))
    )))))

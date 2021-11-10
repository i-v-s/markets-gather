(ns gather.exmo
  (:require
    [clojure.string :as str]
    [manifold.stream :as s]
    [clojure.data.json :as json]
    [aleph.http :as http]
    [gather.common :as c]
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
  (fn [item] (str "spot/" topic ":" (str/replace item "-" "_"))))

(defn dexmo-pair
  "Convert pair name format from Exmo"
  [pair] (str/replace pair "_" "-"))

(defn ws-query
  "Prepare Exmo websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "subscribe"
    :topics (map (pair-topic "trades") trades)
  }))

(defn print-msg
  [ts & args]
  (println (apply str (new java.util.Date ts) " " args)))

(defn get-all-pairs
  []
  (->> "https://api.exmo.com/v1.1/pair_settings"
    c/http-get-json
    keys
    (map dexmo-pair)
  ))

(defn gather-ws
  "Gather from Exmo by websockets"
  [trades put! & {:keys [verbose] :or {verbose :info}}]
  (let [ws @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
    ;(println "Exmo connected")
    ;(println (ws-query trades))
    (s/put-all! ws [(ws-query trades)])
    (while true (let [chunk (json/read-str @(s/take! ws)) {event "event"} chunk]
      ;(println "CHUNK: " chunk " type: " (type chunk))
      (case event
        "info" (let [{ts "ts" code "code" message "message" sid "session_id"} chunk]
          (print-msg ts "Exmo WS: " message "; session " sid "; code " code))
        "error" (let [{ts "ts" code "code" error "error" message "message"} chunk]
          (print-msg ts "Exmo WS error: " message "; code " code))
        "subscribed" (if (= verbose :debug) (let [{ts "ts" topic "topic"} chunk]
          (print-msg ts "Exmo WS subscribed topic: " topic)))
        "update" (let [
          ;topic "spot/trades"
          [topic pair] (str/split (get chunk "topic") #":")
          data (get chunk "data")
          ]
          (case topic
            "spot/trades" (put! (dexmo-pair pair) :t (map transform-trade data))))
        (println "Exmo WS unknown event: " event)
    )))))

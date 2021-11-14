(ns gather.exmo
  (:require
   [clojure.string :as str]
   [manifold.stream :as s]
   [clojure.data.json :as json]
   [aleph.http :as http]
   [gather.common :as c]
   [gather.storage :as sg]))

(def exmo-intervals {:1m "1" :5m "5" :15m "15" :30m "30" :45m "45" :1h "60" :2h "120" :3h "180" :4h "240" :1d "D" :1w "W", :1M "M"})

(defn transform-trade
  "Transform Exmo trade record to Clickhouse row"
  [r]
  [(get r "trade_id")
   (new java.sql.Timestamp (* 1000 (get r "date")))
   (if (= "buy" (get r "type")) 1 0)
   (Float/parseFloat (get r "price"))
   (Float/parseFloat (get r "quantity"))
   (Float/parseFloat (get r "amount"))])  

(defn pair-topic
  "Convert pair to topic"
  [topic pair]
  (str "spot/" topic ":" (str/replace pair "-" "_")))

(defn dexmo-pair
  "Convert pair name format from Exmo"
  [pair] (str/replace pair "_" "-"))

(defn ws-query
  "Prepare Exmo websocket request"
  [pairs]
  (json/write-str {}
                  :id 1
                  :method "subscribe"
                  :topics (for [pair pairs topic ["trades" "order_book_updates"]] (pair-topic topic pair))))
  
(defn print-msg
  [ts & args]
  (println (apply str (new java.util.Date ts) " " args)))

(defn get-all-pairs []
  (->> "https://api.exmo.com/v1.1/currency"
       c/http-get-json
       (map dexmo-pair)))
    
(defn parse-topic
  [topic]
  (let [items (re-find #"^spot/(\w+):(\w+)$" topic)]
    (if (= (count items) 3) (rest items) nil)))

(defrecord Exmo [name intervals-map raw candles]
  sg/Market
  (get-all-pairs [this]
    (->> "https://api.exmo.com/v1.1/pair_settings"
         c/http-get-json
         keys
         (map dexmo-pair)))
  (gather-ws-loop! [{{pairs :pairs trades :trades} :raw} verbose]
    (let [ws @(http/websocket-client "wss://ws-api.exmo.com:443/v1/public")]
      (s/put-all! ws [(ws-query pairs)])
      (while true
        (let [chunk (json/read-str @(s/take! ws)) {event "event"} chunk] ; null!
        ;(println "CHUNK: " chunk " type: " (type chunk))
          (case event
            "info" (let [{ts "ts" code "code" message "message" sid "session_id"} chunk]
                     (print-msg ts "Exmo WS: " message "; session " sid "; code " code))
            "error" (let [{ts "ts" code "code" message "message"} chunk]
                      (print-msg ts "Exmo WS error: " message "; code " code))
            "subscribed" (if (= verbose :debug)
                           (let [{ts "ts" topic "topic"} chunk]
                             (print-msg ts "Exmo WS subscribed topic: " topic)))
            "update" (let [{ts "ts" topic "topic" data "data"} chunk
                           [topic-name pair] (parse-topic topic)]
                       (case topic-name
                         "spot/trades" (sg/push-raw! trades (dexmo-pair pair) (map transform-trade data))
                         ;"spot/order_book_updates" ()
                         (print-msg ts "Exmo WS unknown topic: " topic)))
            (println "Exmo WS unknown event: " event)))))))

(defn create
  "Create Exmo instance"
  [] (Exmo. "Exmo" exmo-intervals nil nil))

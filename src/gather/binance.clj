(ns gather.binance
  (:require
    [clojure.string :as str]
    [manifold.stream :as s]
    [clojure.data.json :as json]
    [aleph.http :as http]
    [byte-streams :as bs]
    [manifold.deferred :as d]
  ))

(defn de-hyphen
  "Remove hyphens from string"
  [item]
  (clojure.string/replace item "-" ""))

(defn lower-pair
  "Convert pair to lower name"
  [item]
  (-> item de-hyphen clojure.string/lower-case))

(defn upper-pair
  "Convert pair to lower name"
  [item]
  (-> item de-hyphen  clojure.string/upper-case))

(defn trade-stream
  "Convert pair to trade topic name"
  [item]
  (str
    (lower-pair item)
    "@trade"))

(defn ws-query
  "Prepare websocket request"
  [trades]
  (json/write-str {
    :id 1
    :method "SUBSCRIBE"
    :params (map trade-stream trades)
  }))

(defn trades-rest-query
  "Prepare REST url request for trades"
  [pair]
  (str
    "https://www.binance.com/api/v3/trades?symbol="
    (upper-pair pair)
    "&limit=1000"))

(defn transform-trade-ws
  "Transform Binance trade record from websocket to Clickhouse row"
  [r]
  (let [
      p (Double/parseDouble (get r "p"))
      q (Float/parseFloat (get r "q"))
    ][
      (get r "t")
      (new java.sql.Timestamp (get r "T"))
      (if (get r "m") 0 1)
      p
      q
      (float (* p q))
    ]))

(defn transform-trade
  "Transform Binance trade record from REST to Clickhouse row"
  [r]
  [
    (get r "id")
    (new java.sql.Timestamp (get r "time"))
    (if (get r "isBuyerMaker") 0 1)
    (Double/parseDouble (get r "price"))
    (Float/parseFloat (get r "qty"))
    (Float/parseFloat (get r "quoteQty"))
  ])

(defn trades-put-map
  "Make callback map for trades streams"
  [conn put-trades! pairs]
  (into {} (map (fn [pair] [
    (trade-stream pair)
    (fn [data]
      (put-trades! conn "Binance" pair [(transform-trade-ws data)]))
  ]) pairs))
  )

(defn recent-trades
  "Get recent trades"
  [pairs put!]
  (doseq [pair pairs]
    (->> @(http/get (trades-rest-query pair))
      :body
      bs/to-string
      json/read-str
      (map transform-trade)
      (put! pair)
      )))

(defn gather
  "Gather from Binance"
  [conn trades put-trades!]
  (let [
      ws @(http/websocket-client (str
        "wss://stream.binance.com:9443/stream?streams="
        (clojure.string/join "/" (map trade-stream trades))))
      actions (trades-put-map conn put-trades! trades)
    ]
    (println "Connected to Binance.")
    (s/put-all! ws [(ws-query trades)])
    (recent-trades trades (fn [p r] (put-trades! conn "Binance" p r)))
    (while true (let [
        chunk (json/read-str @(s/take! ws))
        action (get actions (get chunk "stream"))
      ]
      (if action
        (action (get chunk "data"))
        (println "\nBinance: No action for: " chunk))
    ))))

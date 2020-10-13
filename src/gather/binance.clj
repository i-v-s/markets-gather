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

(defn transform-trade
  "Transform Binance trade record to Clickhouse row"
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

(defn trades-put-map
  "Make callback map for trades streams"
  [conn put-trades! pairs]
  (into {} (map (fn [pair] [
    (trade-stream pair)
    (fn [data]
      (put-trades! conn "Binance" pair [(transform-trade data)]))
  ]) pairs))
  )

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
    (while true (let [
        chunk (json/read-str @(s/take! ws))
        action (get actions (get chunk "stream"))
      ]
      (if action
        (action (get chunk "data"))
        (println "\nBinance: No action for: " chunk))
    ))))

(ns gather.binance
  (:require
    [clojure.string :as str]
    [clojure.walk :as w]
    [clojure.data.json :as json]
    [manifold.stream :as s]
    [aleph.http :as http]
    [byte-streams :as bs]
    [manifold.deferred :as d]
    [gather.common :as c]
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
  [item] (str (lower-pair item) "@trade"))

(defn depth-stream
  "Convert pair to depth topic name"
  [item] (str (lower-pair item) "@depth"))

(defn ws-query
  "Prepare websocket request"
  [trades depths]
  (json/write-str {
    :id 1
    :method "SUBSCRIBE"
    :params (concat
      (map trade-stream trades)
      (map depth-stream depths)
    )
  }))

(defn trades-rest-query
  "Prepare REST url request for trades"
  [pair]
  (str
    "https://www.binance.com/api/v3/trades?symbol="
    (upper-pair pair)
    "&limit=1000"))

(defn depth-rest-query
  "Prepare REST url for depth"
  [pair]
  (str
    "https://www.binance.com/api/v3/depth?symbol="
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
  [r] [
    (get r "id")
    (new java.sql.Timestamp (get r "time"))
    (if (get r "isBuyerMaker") 0 1)
    (Double/parseDouble (get r "price"))
    (Float/parseFloat (get r "qty"))
    (Float/parseFloat (get r "quoteQty"))
  ])

(defn transform-depth-level
  "Transform Binance depth record to Clickhouse row"
  [time id]
  (fn [[p q]] (let [price (Double/parseDouble p)] [
    id
    time
    price
    (-> q Float/parseFloat (* price) float)
    ])))

(defn transform-depth-ws
  "Transform Binance depth records from websocket to Clickhouse rows"
  [{time "E" u1 "U" u2 "u" buy "b" sell "a"}] {
    :b ()
    :s ()
  })

(defn put-map
  "Make callback map for streams"
  [pairs put!]
  (apply hash-map (concat (for [pair pairs] [
    (trade-stream pair)
    (fn [data] (put! pair :t [(transform-trade-ws data)]))
    (depth-stream pair)
    (fn [data] (apply put! pair (transform-depth-ws data)))
  ]))))

(defn put-recent-trades!
  "Get recent trades from REST and put them by callback"
  [pairs put!]
  (doseq [pair pairs]
    (->> @(http/get (trades-rest-query pair))
      :body
      bs/to-string
      json/read-str
      (map transform-trade)
      (put! pair :t)
      )))

(defn transform-depths-rest [d]
  (let [t (transform-depth-level (c/now) (:lastUpdateId d))]
    (concat (for [[k t] [[:bids :b] [:asks :s]] [t (->> d k (map t))]))))

(defn put-current-depths!
  "Get depth from REST and put them by callback"
  [conn pairs put!]
  (doseq [pair pairs]
    (->> @(http/get (depth-rest-query pair))
      :body
      bs/to-string
      json/read-str
      w/keywordize-keys
      transform-depths-rest
      ;(println "DEPTH:")
      (apply put! pair)
      )))

(defn gather
  "Gather from Binance"
  [pairs put!]
  (let [
      ws @(http/websocket-client (str
        "wss://stream.binance.com:9443/stream?streams="
        (clojure.string/join "/" (concat
          (map trade-stream pairs)
          (map depth-stream pairs)
        ))))
      actions (trades-put-map pairs put!)
    ]
    (println "Connected to Binance.")
    (s/put-all! ws [(ws-query pairs pairs)])
    (put-recent-trades! pairs put!)
    (put-current-depths! conn pairs put-depth!)
    (while true (let [
        chunk (json/read-str @(s/take! ws))
        action (get actions (get chunk "stream"))
      ]
      (if action
        (action (get chunk "data"))
        (println "\nBinance: No action for: " chunk))
    ))))

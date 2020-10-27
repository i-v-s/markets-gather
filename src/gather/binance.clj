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
  (-> item de-hyphen clojure.string/upper-case))

(def stream-types {:t "@trade" :d "@depth"})

(defn get-stream
  "Convert pair to stream topic name"
  [type pair] (str (lower-pair pair) (type stream-types)))

(defn ws-query
  "Prepare websocket request"
  [& streams]
  (json/write-str {
    :id 1
    :method "SUBSCRIBE"
    :params (apply concat (for [[type pairs] (apply hash-map streams)]
      (map (partial get-stream type) pairs)))
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
    ][:t [[
      (get r "t")
      (new java.sql.Timestamp (get r "T"))
      (if (get r "m") 0 1)
      p
      q
      (float (* p q))
    ]]]))

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
  [time]
  (let [ts (new java.sql.Timestamp time)]
    (fn [[p q]] (let [price (Double/parseDouble p)] [
      ts
      price
      (-> q Float/parseFloat (* price) float)
      ]))))

(defn transform-depth-ws
  "Transform Binance depth records from websocket to Clickhouse rows"
  [{time "E" buy "b" sell "a"}]
    (apply concat (for [[k vs] {:b buy :s sell}]
      [k (map (transform-depth-level time) vs)])))

(def transforms-ws {
  :t transform-trade-ws
  :d transform-depth-ws
  })

(defn put-map
  "Make callback map for streams"
  [pairs put!]
  (->> (for [pair pairs [tp tf] transforms-ws] [
    (get-stream tp pair)
    (fn [data] (apply put! pair (tf data)))
  ]) (into {})))

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
  (let [tf (transform-depth-level (System/currentTimeMillis))]
    (apply concat (for [[k t] {:bids :b :asks :s}] [t (->> d k (map tf))]))
    ))

(defn print-pass
  [v] (println "PASS:" v) v)

(defn put-current-depths!
  "Get depth from REST and put them by callback"
  [pairs put!]
  (doseq [pair pairs]
    (->> @(http/get (depth-rest-query pair))
      :body
      bs/to-string
      json/read-str
      w/keywordize-keys
      transform-depths-rest
      ;print-pass
      (apply put! pair)
      )))

(defn gather
  "Gather from Binance"
  [pairs put!]
  (let [
      ws @(http/websocket-client (str
        "wss://stream.binance.com:9443/stream?streams="
        (clojure.string/join "/" (concat
          (map (partial get-stream :t) pairs)
          (map (partial get-stream :d) pairs)
        ))))
      actions (put-map pairs put!)
    ]
    (println "Binance connected")
    (s/put-all! ws [(ws-query :t pairs :d pairs)])
    (put-recent-trades! pairs put!)
    (put-current-depths! pairs put!)
    (while true (let [
        chunk (json/read-str @(s/take! ws))
        action (get actions (get chunk "stream"))
      ]
      (if action
        (action (get chunk "data"))
        (println "\nBinance: No action for: " chunk))
    ))))

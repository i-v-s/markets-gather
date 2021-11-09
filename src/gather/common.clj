(ns gather.common
  (:require
    [clojure.string :as str]
    [clojure.core.async :as a]
    [clojure.java.shell :as sh]
    [clojure.data.json :as json]
    [clojure.tools.cli :refer [parse-opts]]
    [byte-streams :as bs]
    [aleph.http :as http]
  ))

(defn lower
  "Convert name to lowercase and '-' to '_'"
  [name]
  (clojure.string/lower-case (clojure.string/replace name "-" "_")))

(def capitalize-key
  "Converts :key to \"Key\""
  (comp str/capitalize name))

(defn comma-join
  "Join items with comma"
  [items]
  (clojure.string/join ", " items))

(def table-types {
  :t "trades"
  :b "buy"
  :s "sell"
  :p "prices"
  })

(defn url-encode-params
  "Encode params in url"
  [url & params]
  (if
    (empty? params)
    url
    (->> params
      (apply hash-map)
      (filter #(some? (last %)))
      (map (fn [[k v]] (str (name k) "=" v)))
      (clojure.string/join "&")
      (str url "?")
    )))

(defn http-get-json
  "Get JSON data with HTTP GET request"
  [url & params]
  (->>
    (apply url-encode-params url params)
    http/get
    deref
    :body
    bs/to-string
    json/read-str
  ))

(defn wc-test
  "Wildcard test"
  [wc]
  (cond
    (= wc "*") (constantly true)
    (str/starts-with? wc "*") (fn [s] (str/ends-with? s (subs wc 1)))
    ))

(defn get-table-name
  "Get table name for trades or depths"
  [db market item type]
  (str db "." (lower (name market)) "_" (lower item) "_" (type table-types)))

(defn try-loop
  "Try to call function in loop"
  [func & {:keys [title delay] :or {delay 1000}}]
  (loop []
    (try
       (func)
       (catch Exception e
         (println "\n" title "exception:")
         (clojure.stacktrace/print-stack-trace e)))
    (Thread/sleep delay)
    (recur)))

(defn forever-loop
  "Execute function in async thread loop"
  [func & args]
  (a/thread (apply try-loop func args)))

(defn shutdown-hook
  [f]
  (.addShutdownHook (Runtime/getRuntime) (Thread. (f))))

(defn now
  "Return current time"
  []
  (new java.sql.Timestamp (System/currentTimeMillis)))

(defn throttle
  [ms f!]
  (let [last (atom 0)]
    (fn [& args]
      (let [t (System/currentTimeMillis)]
        (if (> t @last)
          (do (apply f! args) (reset! last (+ ms t))))))))

(defn map-sum
  "Summarize hashmap values by key aggregation"
  [m & {:keys [k v] :or {v deref}}]
  (reduce-kv
    (fn [m ki vi]
      (let [kr (k ki)]
        (assoc m kr (+ (v vi) (get m kr 0)))))
    {} m))

(defn vec-to-map-of-vec
  "Convert vector of records to map of vectors"
  [key value coll]
  (reduce
    (fn [coll item]
      (let [kr (key item)]
        (assoc coll kr (conj (get coll kr []) (value item)))))
    {} coll))

(defn filter-keys
  [f coll]
  (reduce-kv
    (fn [coll k v]
      (if (f k) (assoc coll k v) coll)
      {} coll)))

(defn to-uint [s] (Integer/parseUnsignedInt s))

(defn within
  [a b v]
  (and
    (>= v a)
    (<= v b)))

(defn exec!
  [& args]
  (let [{exit :exit err :err out :out} (apply sh/sh args)]
    (if (not= exit 0) (throw (Exception. err)) out)))

(defn pwd "Current directory" [] (-> "pwd" exec! str/trim))

(defn ls [& args] (str/split-lines (apply exec! "ls" args)))

(defn extract! [archive destination]
  (exec! "tar" "-xvf" archive :dir destination))

(defn mv! [dst & src]
  (apply exec! "mv" "-t" dst src))

(defn mv-all! [src dst]
  (->> src ls (map (partial str src "/")) (apply mv! dst)))

(defn select-values [map ks]
  (reduce #(conj %1 (map %2)) [] ks))

(def cli-options {
  :config ["-c" "--config CONFIG" "Config file name"
    :default nil
    ;:parse-fn #(Integer/parseInt %)
    ;:validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]
    ]
   ;; A non-idempotent option (:default is applied first)
  ; ["-v" nil "Verbosity level"
  ;  :id :verbosity
  ;  :default 0
  ;  :update-fn inc] ; Prior to 0.4.1, you would have to use:
                   ;; :assoc-fn (fn [m k _] (update-in m [k] inc))
   ;; A boolean option defaulting to nil
  :help ["-h" "--help"]
  })

(defn select-options [&keys] (select-values cli-options keys))

(defn parse-options [args & keys] (parse-opts args (select-values cli-options keys)))

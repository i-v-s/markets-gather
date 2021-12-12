(ns gather.common
  (:require
   [clojure.string :as str]
   [clojure.core.async :as a]
   [clojure.java.shell :as sh]
   [clojure.data.json :as json]
   [clojure.set :refer [union]]
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.stacktrace :refer [print-stack-trace]]
   [byte-streams :as bs]
   [aleph.http :as http]))

(import
 [java.time LocalDateTime ZoneOffset])

(def intervals-map {
  :1m 60 :3m 180 :5m 300 :15m 900 :30m 1800 
  :1h 3600 :2h 7200 :4h 14400 :6h (* 6 3600) :8h (* 8 3600) :12h (* 12 3600)
  :1d 86400 :3d (* 3 86400) :1w (* 7 86400) :1M (* 31 86400) :1mo (* 31 86400)})

(defn candle-name [k] (str/replace (name k) "M" "mo"))

(defn lower
  "Convert name to lowercase and '-' to '_'"
  [name]
  (clojure.string/lower-case (clojure.string/replace name "-" "_")))

(defn hyphen-split [s] (str/split s #"-"))

(def capitalize-key
  "Converts :key to \"Key\""
  (comp str/capitalize name))

(defn comma-join
  "Join items with comma"
  [items]
  (clojure.string/join ", " items))

(def table-types {:t "trades"
                  :b "buy"
                  :s "sell"
                  :p "prices"
                  :c "candles"})

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

(defn group-by-contains
  [possible values]
  (let [{not-exists false exists true}
        (group-by (partial contains? possible) values)]
    [exists not-exists]))

(defn ts-max [& args]
  (let [args' (filter some? args)]
    (if (not-empty args')
      (apply max args')
      nil)))

(defn set-of-keys
  [items]
  (transduce (map (comp set keys)) union items))

(defn ts-to-long
  [ts]
  (if (nil? ts) nil (.getTime ts)))

(defn ts-to-seconds [ts] (long (/ (ts-to-long ts) 1000)))

(defn str-some [& args] (apply str (filter some? args)))

(defn ts-str
  [ts & args]
  (if (nil? ts)
    "<nil>"
    (apply str (java.sql.Timestamp. ts) args)))

(defmulti format-ldt (fn [tf _] (get {:1mo :1M :1M :1M :1w :1d :3d :1d :1d :1d :4h :1h :1h :1h} tf :1m)))
(defmethod format-ldt :1M [_ dt] (format "%4d-%02d" (.getYear dt) (.getValue (.getMonth dt))))
(defmethod format-ldt :1d [_ dt] (format "%4d-%02d-%02d" (.getYear dt) (.getValue (.getMonth dt)) (.getDayOfMonth dt)))
(defmethod format-ldt :1h [_ dt] (format "%4d-%02d-%02d %02d" (.getYear dt) (.getValue (.getMonth dt)) (.getDayOfMonth dt) (.getHour dt)))
(defmethod format-ldt :1m [_ dt] (format "%4d-%02d-%02d %02d:%02d"
                                         (.getYear dt) (.getValue (.getMonth dt)) (.getDayOfMonth dt) (.getHour dt) (.getMinute dt)))

(defmulti format-ts (fn [_ ts] (type ts)))
(defmethod format-ts java.sql.Timestamp [tf ts] (format-ldt tf (.toLocalDateTime ts)))
(defmethod format-ts Long [tf ts] (format-ldt tf (.toLocalDateTime (java.sql.Timestamp. ts))))

(defn format-interval
  [tf a b]
  (if (= a b)
    (format-ts tf a)
    (str (format-ts tf a) " - " (format-ts tf b))))

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

(defn re-rest [pat] (comp rest (partial re-find pat)))
(defn re-one [pat s] (second (re-find pat s)))

(defn get-table-name
  "Get table name for trades or depths"
  [market-name item type]
  (str (lower market-name) "_" (lower item) "_" (type table-types)))

(defn try-loop
  "Try to call function in loop"
  [func & {:keys [title delay] :or {delay 1000}}]
  (loop []
    (try
       (func)
       (catch Exception e
         (println "\n" title "exception:")
         (print-stack-trace e)))
    (Thread/sleep delay)
    (recur)))

(defmacro with-retry
  "body must return non false value"
  [tries & body]
  (let [e (gensym 'e) left (gensym 'left) result (gensym 'result) wait (gensym 'wait)]
    `(let [~result (atom nil)]
       (loop [~left (dec ~tries)]
         (when
          (try
            (reset! ~result (do ~@body))
            false
            (catch clojure.lang.ExceptionInfo ~e
              (if-let [~wait (-> ~e ex-data :retry-after)]
                (do
                  (println (str "\nException (tries left " ~left "): " (ex-message ~e)))
                  (if (pos? ~left)
                    (do
                      (Thread/sleep ~wait)
                      true)
                    (throw ~e)))
                (throw ~e))))
           (recur (dec ~left))))
       @~result)))

(defn forever-loop
  "Execute function in async thread loop"
  [func & args]
  (a/thread (apply try-loop func args)))

(defn shutdown-hook
  [f]
  (.addShutdownHook (Runtime/getRuntime) (Thread. (f))))

(defn now-ts [] (System/currentTimeMillis))
(defn make-ts [ts] (java.sql.Timestamp. ts))
(def make-ts-sec (comp make-ts (partial * 1000)))
(def now "Return current time" (comp make-ts now-ts))

(defn inc-ts
  [ts tf & {:keys [mul] :or {mul 1}}]
  (case tf
    :1M (-> (LocalDateTime/ofEpochSecond (long (/ ts 1000)) 0 ZoneOffset/UTC)
            (.plusMonths mul)
            (.toEpochSecond ZoneOffset/UTC)
            (* 1000))
    (+ ts (* mul 1000 (tf intervals-map)))))

(defn dec-ts
  [ts tf & {:keys [mul] :or {mul 1}}]
  (inc-ts ts tf :mul (- mul)))

(defn throttle
  [ms f!]
  (let [last (atom 0)]
    (fn [& args]
      (let [t (System/currentTimeMillis)]
        (when (> t @last)
          (apply f! args)
          (reset! last (+ ms t)))))))

(defn make-wc
  [item]
  (re-pattern
   (str "^" (str/replace item "*" "\\w+") "$")))

(defn some-fn-map
  "Combines some-f n with map. (some-fn-map f coll) = (apply some-fn (map f coll))"
  [f items]
  (let [fns (map f items)]
    (fn [arg]
      (loop [i fns]
        (if-let [v (first i)]
          (if (v arg)
            true
            (recur (rest i)))
          false)))))

(defn wildcard-checker
  [& args]
  (if (not-empty args)
   (->> args
       (map make-wc)
       (some-fn-map (partial partial re-find)))
    any?))

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

(defn to-uint [s] (Integer/parseUnsignedInt s))

(defn within
  [a b v]
  (and
    (>= v a)
    (<= v b)))

(def cons-nil (partial cons nil))
(defn conj-nil [v] (concat v [nil]))
(defn not-and [[a b]] (not (and a b)))

(defn ajacent-filter [f items]
  (sequence
   (comp
    (map list)
    (filter
     (some-fn
      not-and
      (partial apply f))))
   (cons-nil items)
   (conj-nil items)))

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

(def cli-options
  {:config ["-c" "--config CONFIG" "Config file name" :default nil
    ;:parse-fn #(Integer/parseInt %)
    ;:validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]
            ]
   :db-url ["-u" "--db-url URL" "Clickhouse database url" :default nil]
   ;; A non-idempotent option (:default is applied first)
   :verbosity ["-v" nil "Verbosity level"
               :id :verbosity
               :default 0
               :update-fn inc]
   ;; A boolean option defaulting to nil
   :help ["-h" "--help"]})

(defn parse-options [args & keys] (parse-opts args (select-values cli-options keys)))

(ns gather.ch
  (:require     [clojure.string :as str]
                [gather.common :as c]))

(import java.sql.DriverManager)

(defn connect
  "Connect to Clickhouse"
  [url]
  (DriverManager/getConnection url))

(defn connect-st
  "Connect to Clickhouse"
  [url & args]
  (.createStatement (apply connect url args)))

(defmulti exec! "Execute SQL query" (fn [c _] (class c)))

(defn use!
  "Execute USE statement"
  [c db]
  (exec! c (str "USE " db)))

(defn parse-url
  [url]
  (if-let [items (re-find #"^(jdbc:clickhouse://[\w\-:\.]+)/(\w+)$" url)]
    (rest items) nil))

(defn create-db!
  "Execute CREATE DATABASE statement"
  ([c db]
  (exec! c (str "CREATE DATABASE IF NOT EXISTS " db)))
  ([url]
   (when-let [[default-url db] (parse-url url)]
     (create-db! (connect default-url) db))))

(defmethod exec! com.github.housepower.jdbc.statement.ClickHouseStatement
  [stmt query]
  (try
    (.executeQuery stmt query)
    (catch Exception e
      (println "\nException during SQL execution. Query was:")
      (println query)
      (throw e))))

(defmethod exec! com.github.housepower.jdbc.ClickHouseConnection [c query]
  (exec! (.createStatement c) query))

(defn get-metadata
  [result-set]
  (let [md (.getMetaData result-set)]
    (->> md
         .getColumnCount
         inc
         (range 1)
         (map
          (fn [i]
            [(->> i (.getColumnLabel md) keyword)
             (.getColumnTypeName md i)])))))

(defmulti read-result-set (fn [t _ _] t))
(defmethod read-result-set "UInt8" [_ rs j] (.getLong rs j))
(defmethod read-result-set "String" [_ rs j] (.getString rs j))
(defmethod read-result-set "Nullable(DateTime)" [_ rs j] (.getTimestamp rs j))
(defmethod read-result-set "DateTime" [_ rs j] (.getTimestamp rs j))

(defn fetch-row-hm
  [result-set metadata]
  (->> metadata
       (map-indexed
        (fn [i [k t]]
          (let [j (inc i)]
            [k (read-result-set t result-set j)])))
       (into {})))

(defn fetch-one
  [result-set]
  (let [md (.getMetaData result-set)]
    (.next result-set)
    (for [i (range 1 (inc (.getColumnCount md)))]
      (read-result-set (.getColumnTypeName md i) result-set i))))

(defn current-db
  [c]
  (->> "SELECT currentDatabase()"
       (exec! c)
       fetch-one
       first))

(defn fetch-all
  "Fetch all rows from result-set"
  [result-set]
  (let [md (get-metadata result-set)]
    (for [_ (range) :while (.next result-set)]
      (fetch-row-hm result-set md))))

(defn fetch-tables
  "Fetch all table names from current DB"
  [c]
  (->> "SHOW TABLES" (exec! c) fetch-all (map :name)))

(defn descr-to-rec
  [{name :name type :type def-expr :default_expression codec :codec_expression}]
  [(keyword name)
   (->> [type
         def-expr
         (if (not-empty codec)
           (str "CODEC(" codec ")")
           nil)]
        (filter not-empty)
    (str/join " " ))])

(defn describe-table [c table]
    (->> table (str "DESCRIBE TABLE ") (exec! c) fetch-all (map descr-to-rec) (into {})))

(defn show-table [c table]
  (->> table (str "SHOW CREATE TABLE ") (exec! c) fetch-all first :statement))

(defn db-table-key
  [item] (str (:database item) "." (:table item)))

(defn fetch-partitions
  "Fetch partitions"
  [st & {:keys [db]}]
  (->>
    (str
      "SELECT partition, database, table "
      "FROM system.parts "
      (if db (str "WHERE database = '" db "' ") "")
      "GROUP BY partition, database, table "
      "ORDER BY database, table, partition")
    (exec! st)
    fetch-all
    (c/vec-to-map-of-vec (if db :table db-table-key) :partition)))

(defn exec-vec!
  "Execute vec of queries"
  [conn queries]
  (dorun
    (map
      (partial exec! (.createStatement conn))
      queries)))

(defmulti set-pst-item class)
(defmethod set-pst-item java.lang.Float    [_] (fn [p-st i v] (.setFloat p-st i v)))
(defmethod set-pst-item java.lang.Double   [_] (fn [p-st i v] (.setDouble p-st i v)))
(defmethod set-pst-item java.lang.Boolean  [_] (fn [p-st i v] (.setBoolean p-st i v)))
(defmethod set-pst-item java.lang.Long     [_] (fn [p-st i v] (.setLong p-st i v)))
(defmethod set-pst-item java.lang.String   [_] (fn [p-st i v] (.setString p-st i v)))
(defmethod set-pst-item java.sql.Timestamp [_] (fn [p-st i v] (.setTimestamp p-st i v)))

(defn set-pst-item!
  [p-st i v]
  ((set-pst-item v) p-st i v))

(defn add-batch!
  "Add row of data to prepared statement"
  [p-st values]
  (doseq [[i v] (map-indexed vector values)]
    (set-pst-item! p-st (+ i 1) v))
  (.addBatch p-st))

(defn insert-many!
  "Insert many rows of data"
  [conn query items]
  (let [p-st (.prepareStatement conn query)]
    (doseq [item items]
      (add-batch! p-st item))
    (.executeBatch p-st)))

(defn conj-fields
  "Add field(s) to table record"
  [rec & fields]
  (apply vector
   (->> fields
        (apply concat (first rec))
        (apply concat)
        (apply array-map))
   (rest rec)))

(defn column-desc-query
  [[key desc]]
  (str (name key) " " desc))

(defn create-table-query
  "Return create table query"
  [name rec & {
    :keys [order-by engine partition-by settings]
    :or {
      engine "MergeTree()"
      settings []
    }
    }]
  (str
    "CREATE TABLE IF NOT EXISTS " name " ("
    (c/comma-join (map column-desc-query rec))
    ") ENGINE = " engine
    " ORDER BY (" (c/comma-join order-by) ")"
    (if partition-by (str " PARTITION BY " partition-by) "")
    (if (empty? settings) "" (str " SETTINGS " (c/comma-join settings)))
    ))

(defn ensure-tables!
  "Ensure that tables created and have all needed columns"
  [conn tables-map]
  (let [all-tables (set (fetch-tables conn))
        [tabs-check tabs-create] (c/group-by-contains all-tables (keys tables-map))]
    (doseq [name tabs-create]
      (->> name
           tables-map
           (apply create-table-query name)
           (exec! conn)))
    (doseq [tab-name tabs-check
            :let [cols (first (tables-map tab-name))
                  desc (describe-table conn tab-name)
                  [cols-check cols-create] (c/group-by-contains desc (keys cols))
                  cols-check' (map (juxt identity cols desc) cols-check)
                  cols-bad (filter (fn [[_ a b]] (not= a b)) cols-check')]]
      (when (not-empty cols-bad)
        (println "Warning ch/ensure-tables! - column types mismatch. Table" tab-name "cols" cols-bad))
      (when (not-empty cols-create)
        (->> cols-create
             (select-keys cols)
             (map (comp (partial str "ADD COLUMN ") column-desc-query))
             (str/join ", ")
             (str "ALTER TABLE " tab-name " ")
             (exec! conn))
        (println "Info ch/ensure-tables! - table" tab-name "created cols:" (str/join ", " (map name cols-create)))))))

(defn insert-query
  [table rec]
  (str
    "INSERT INTO " table " ("
    (c/comma-join (for [[key _] rec] (name key)))
    ") VALUES ("
    (-> rec count (repeat "?") c/comma-join)
    ")"))

(defn copy-table
  [st from to]
  (println "Creating table" to)
  (exec! st (str "CREATE TABLE IF NOT EXISTS" to " AS " from))
  (println "Coping from" from "to" to)
  (exec! st (str "INSERT INTO " to " SELECT * FROM " from)))

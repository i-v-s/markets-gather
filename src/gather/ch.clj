(ns gather.ch
  (:require [hugsql.core :as hugsql]
            [hikari-cp.core :refer [close-datasource make-datasource]]
            [clojure.java.jdbc :as jdbc]
            [hugsql.adapter.clickhouse-native-jdbc :as clickhouse]
            [gather.common :as c]))

(import java.sql.DriverManager)

(defn connect
  "Connect to Clickhouse"
  [url]
  (DriverManager/getConnection url))

(defn connect-st
  "Connect to Clickhouse"
  [url]
  (.createStatement (DriverManager/getConnection url)))

(defn exec-query!
  "Execute SQL query"
  [stmt query]
  (try
    (.executeQuery stmt query)
    (catch Exception e
      (println "\nException during SQL execution. Query was:")
      (println query)
      (throw e)
  )))

(defn get-metadata
  [result-set]
  (let [md (.getMetaData result-set)]
    (->> md .getColumnCount inc (range 1) (map (fn [i] [
      (->> i (.getColumnLabel md) keyword)
      (.getColumnTypeName md i)
    ]))
    ;(into {})
    )))

(defn fetch-row
  [result-set metadata]
  (->> metadata (map-indexed (fn [i [k t]]
    (let [j (inc i)] {k (case t
      "UInt8" (.getLong result-set j)
      "String" (.getString result-set j)
      )}))) (into {})))

(defn fetch-all
  "Fetch all rows from result-set"
  [result-set]
  (let [md (get-metadata result-set)]
    (for [i (range) :while (.next result-set)] (fetch-row result-set md))))

(defn fetch-tables
  [st db]
  (exec-query! st (str "USE " db))
  (->> "SHOW TABLES" (exec-query! st) fetch-all (map :name)))

(defn show-table
  [st table] (->> table
    (str "SHOW CREATE TABLE ") (exec-query! st) fetch-all first :statement))

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
    (exec-query! st)
    fetch-all
    (c/vec-to-map-of-vec (if db :table db-table-key) :partition)))

(defn exec-vec!
  "Execute vec of queries"
  [conn queries]
  (dorun
    (map
      (partial exec-query! (.createStatement conn))
      queries)))

(defmulti set-pst-item class)
(defmethod set-pst-item java.lang.Float    [x] (fn [p-st i v] (.setFloat p-st i v)))
(defmethod set-pst-item java.lang.Double   [x] (fn [p-st i v] (.setDouble p-st i v)))
(defmethod set-pst-item java.lang.Boolean  [x] (fn [p-st i v] (.setBoolean p-st i v)))
(defmethod set-pst-item java.lang.Long     [x] (fn [p-st i v] (.setLong p-st i v)))
(defmethod set-pst-item java.lang.String   [x] (fn [p-st i v] (.setString p-st i v)))
(defmethod set-pst-item java.sql.Timestamp [x] (fn [p-st i v] (.setTimestamp p-st i v)))

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

(defn column-desc-query
  [[key desc]]
  (str (name key) " " desc))

(defn create-table-query
  "Return create table query"
  [name rec & {
    :keys [order-by engine partition-by settings]
    :or {
      engine "MergeTree()"
      partition-by "toYYYYMM(time)"
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

(defn insert-query
  [table rec]
  (str
    "INSERT INTO " table " ("
    (c/comma-join (for [[key desc] rec] (name key)))
    ") VALUES ("
    (c/comma-join (for [a rec] "?"))
    ")"))

(defn copy-table
  [st from to]
  (println "Creating table" to)
  (exec-query! st (str "CREATE TABLE IF NOT EXISTS" to " AS " from))
  (println "Coping from" from "to" to)
  (exec-query! st (str "INSERT INTO " to " SELECT * FROM " from)))

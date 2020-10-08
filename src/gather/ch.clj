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
  (.executeQuery stmt query))

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

(defn column-query
  [item]
  (let [[key desc] item]
    (str (name key) " " desc)))

(defn create-table-query
  "Return create table query"
  [name rec & {
    :keys [order-by engine partition-by]
    :or {
      engine "MergeTree()"
      partition-by "toYYYYMM(time)"
    }
    }]
  (str
    "CREATE TABLE IF NOT EXISTS " name "("
    (clojure.string/join ", " (map column-query rec))
    ") ENGINE = " engine
    " ORDER BY " (c/comma-join order-by)
    (if partition-by (str " PARTITION BY " partition-by) "")
    ))

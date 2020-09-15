(ns gather.ch
  (:require [hugsql.core :as hugsql]
            [hikari-cp.core :refer [close-datasource make-datasource]]
            [clojure.java.jdbc :as jdbc]
            [hugsql.adapter.clickhouse-native-jdbc :as clickhouse]
            [gather.common :as c]))

(import java.sql.DriverManager)

;(hugsql/def-db-fns "fns.sql")
;(hugsql/set-adapter! (clickhouse/hugsql-adapter-clickhouse-native-jdbc))


; (def conn (DriverManager/getConnection "jdbc:clickhouse://127.0.0.1:9000"))
; (def stmt (.createStatement conn))


(defn connect
  "Connect to Clickhouse"
  [url]
  (DriverManager/getConnection url))

(defn connect-st
  "Connect to Clickhouse"
  [url]
  (.createStatement (DriverManager/getConnection url)))

;  (make-datasource
;                   {:jdbc-url url}))

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

(defn add-batch!
  [p-st values]
  (doseq [[i v] (map-indexed seq values)]
    (case (type v)
      java.lang.String (.setString p-st (+ i 1) v)
      java.lang.Long (.setLong p-st (+ i 1) v)
    ))
  (.addBatch p-st))

(defn insert-many!
  [conn query items]
  "Insert many rows of data"
  (let [p-st (.prepareStatement conn query)])
  (doseq [item items]
    add-batch! p-st item)
  (.executeBatch p-st))

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


; (import java.sql.DriverManager)
; (def conn (DriverManager/getConnection "jdbc:clickhouse://127.0.0.1:9000"))
; (def stmt (.createStatement conn))
; (def rs (.executeQuery stmt "SELECT 5"))
; (.next rs)
; (.getInt rs 1)

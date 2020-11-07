(ns gather.core
  (:require
    [gather.ch :as ch]))

(defn calc-prices-query
  [table where & {:keys [limit] :or {limit nil}}]
  (str
    "WITH buy AS (SELECT * FROM " table " WHERE " where
    (if limit (str "ORDER BY time LIMIT " limit) "") ")"
    "SELECT time, MAX(price) FROM ("
      "SELECT t.time, MAX(buy.time) AS t, buy.price"
      "FROM (SELECT time FROM buy GROUP BY time) AS t"
      "CROSS JOIN buy WHERE t.time >= buy.time"
      "GROUP BY t.time, buy.price"
    ") AS m INNER JOIN ("
      "SELECT time AS t, price"
      "FROM buy WHERE base > 0"
    ") AS b USING (t, price) GROUP BY time ORDER BY time"
    ))

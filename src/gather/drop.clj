(ns gather.drop
  (:require
    [gather.common :as c]
    [gather.ch :as ch]))

(defn -main
  "Start with params"
  [db-url args]
  (let [
      st (ch/connect-st db-url)
      tabs (ch/fetch-tables st)
      f-tabs (vec (distinct (mapcat
        (fn [arg] (filter (c/wc-test arg) tabs))
        args)))
    ]
    (println "Wildcards:" args)
    ;(println "Tabs to drop:"  f-tabs)
    (doseq [tab f-tabs]
      (println "Dropping " tab)
      (ch/exec-query! st (str "DROP TABLE fx." tab))
      )))

(ns gather.drop
  (:require
    [clojure.string :as str]
    [gather.ch :as ch]))

(defn wc-test
  "Wildcard test"
  [wc]
  (if (str/starts-with? wc "*") (fn [s] (str/ends-with? s (subs wc 1)))))

(defn -main
  "Start with params"
  [db-url args]
  (let [
      st (ch/connect-st db-url)
      tabs (ch/fetch-tables st)
      f-tabs (vec (distinct (mapcat
        (fn [arg] (filter (wc-test arg) tabs))
        args)))
    ]
    (println "Wildcards:" args)
    ;(println "Tabs to drop:"  f-tabs)
    (doseq [tab f-tabs]
      (println "Dropping " tab)
      (ch/exec-query! st (str "DROP TABLE fx." tab)))
    ))

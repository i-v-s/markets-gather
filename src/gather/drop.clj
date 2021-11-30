(ns gather.drop
  (:require
   [clojure.string :as str]
   [gather.common :as c]
   [gather.ch :as ch]))

(defn make-wc
  [item]
  (re-pattern
   (str "^" (str/replace item "*" "\\w+") "$")))

(defn -main
  "Start with params"
  [db-url args]
  (if (empty? args)
    (println "No wildcards specified")
    (let [wcs (map make-wc args)
          checker (c/some-fn-map (partial partial re-find) wcs)
          st (ch/connect-st db-url)
          tabs (ch/fetch-tables st)
          f-tabs (filter checker tabs)]
      (println "Database URL is:" db-url)
      (println "Using expressions:" wcs)
      (if (empty? f-tabs)
        (println "No tables to drop")
        (do
          (println "Tables to drop:")
          (println (c/comma-join f-tabs))
          (println "Are you sure? (y/n)")
          (when (= (read-line) "y")
            (doseq [tab f-tabs]
              (println "Dropping " tab)
              (ch/exec! st (str "DROP TABLE " tab)))))))))

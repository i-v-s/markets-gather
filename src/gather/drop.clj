(ns gather.drop
  (:require [house.ch      :as ch]
            [gather.common :as c]))

(defn -main
  "Start with params"
  [db-url args]
  (if (empty? args)
    (println "No wildcards specified")
    (let [wcs (map c/make-wc args)
          checker (c/some-fn-map (partial partial re-find) wcs)
          conn (ch/connect db-url)
          tabs (ch/fetch-tables conn)
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
              (ch/exec! conn (str "DROP TABLE " tab)))))))))

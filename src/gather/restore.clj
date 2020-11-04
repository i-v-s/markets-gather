(ns gather.restore
  (:require
    [clojure.string :as str]
    [gather.ch :as ch]
    [gather.common :as c]
    [clojure.java.shell :as sh]
    [clojure.java.io :as io]))

(def ch-dir "/var/lib/clickhouse")

(defn -main
  "Start with params"
  [db-url file-name]
  (let [
      st (ch/connect-st db-url)
      data-dir (str ch-dir "/data/restore")
      meta-dir (str ch-dir "/metadata/restore")
      reme-dir (str data-dir "/metadata")
      ]
      (try
        (println "Create database 'restore'")
        (ch/exec-query! st "CREATE DATABASE restore ENGINE = Ordinary")
        (println "Extracting data")
        (c/extract! (str (c/pwd) "/" file-name) data-dir)
        (c/exec! "chown" "-R" "clickhouse:clickhouse" data-dir)
        (c/mv-all! reme-dir meta-dir)
        (c/exec! "rmdir" reme-dir)
        (println "Attaching tables")
        (doseq [table (c/ls data-dir)]
          (ch/exec-query! st (str "ATTACH TABLE restore." table)))
        (println "Merge DB 'restore' to 'fx'")
        (doseq [
          [table parts] (ch/fetch-partitions st :db "restore")
          part parts]
          (println "Moving" table part)
          (ch/exec-query! st (str
            "ALTER TABLE restore." table
            " MOVE PARTITION " part
            " TO TABLE fx." table)))
        (println "Remove database 'restore'")
        (ch/exec-query! st "DROP DATABASE restore")
        (println "Completed")
      (catch Exception e
        (println "\nException:" (.getMessage e))))))

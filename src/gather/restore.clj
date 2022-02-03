(ns gather.restore
  (:require [clojure.string :as str]
            [gather.ch :as ch]
            [gather.common :as c]))

(def user-dir "/var/lib/clickhouse/user_files/")

(defn -main
  "Start with params"
  [db-url file-name]
  (let [st (ch/connect-st db-url)]
    (try
      (println "Create database 'restore'")
      (ch/exec! st "CREATE DATABASE restore ENGINE = Ordinary")
      (println "Extracting data")
      (c/extract! file-name user-dir)
      (c/exec! "chown" "-R" "clickhouse:clickhouse" user-dir)
      (println "Attaching tables")
      (doseq [table (c/ls user-dir)
              :let [file (str user-dir table "/create.sql")
                    query (-> file
                              slurp
                              (str/replace-first
                               #"CREATE\s+TABLE\s+\w+\.(\w+)\s+\("
                               (str "ATTACH TABLE restore.$1 FROM '" user-dir table "/' ("))
                              (str/replace-first
                               #"\s+storage_policy\s+=\s+'\w+'," ""))]]
        (println "Attaching table" table)
        (c/exec! "rm" file)
        (ch/exec! st query))
      (println "Restore completed")
      (System/exit 0)

      (println "Merge DB 'restore' to 'fx'")
      (doseq [[table parts] (ch/fetch-partitions st :db "restore") part parts]
        (println "Moving" table part)
        (try
          (ch/exec! st (str
                        "ALTER TABLE restore." table
                        " MOVE PARTITION " part
                        " TO TABLE fx." table))
          (catch Exception e (println (.getMessage e))))
        (println (str "Try to optimize table fx." table) part)
        (ch/exec! st (str
                      "OPTIMIZE TABLE fx." table
                      " PARTITION " part
                      " FINAL")))
      (println "Remove database 'restore'")
      (ch/exec! st "DROP DATABASE restore")
      (println "Completed")
      (catch Exception e
        (println "\nException:" (.getMessage e)))))
  (System/exit 0))

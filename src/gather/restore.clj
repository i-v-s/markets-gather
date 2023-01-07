(ns gather.restore
  (:require [clojure.string :as str]
            [house.ch       :as ch]
            [gather.common  :as c]))

(def user-dir "/var/lib/clickhouse/user_files/")

(defn fetch-dbs [conn] (->> "SHOW DATABASES" (ch/fetch-all conn) (map first)))
(defn drop-db! [conn db] (ch/exec! conn (str "DROP DATABASE " db)))

(defn -main
  "Start with params"
  [db-url file-name & {:keys [temp-db] :or {temp-db "restore"}}]
  (ch/create-db! db-url)
  (let [dest-db (-> db-url ch/parse-url last)
        conn (ch/connect db-url)
        dest-tables (-> conn ch/fetch-tables set)]
    (try
      (when (some (partial = temp-db) (fetch-dbs conn))
        (println "Warning: database" temp-db "exists. Drop? (y/n)")
        (if (= (read-line) "y")
          (drop-db! conn temp-db)
          (c/exit! 1)))
      (println (str "Create database '" temp-db "'"))
      (ch/exec! conn (str "CREATE DATABASE " temp-db))
      (println "Extracting data")
      (c/extract! file-name user-dir)
      (c/exec! "chown" "-R" "clickhouse:clickhouse" user-dir)
      (doseq [table (c/ls user-dir)
              :let [file (str user-dir table "/create.sql")
                    query (-> file
                              slurp
                              (str/replace-first
                               #"CREATE\s+TABLE\s+\w+\.(\w+)\s+\("
                               (str "ATTACH TABLE " temp-db ".$1 FROM '" user-dir table "/' ("))
                              (str/replace-first
                               #"\s+storage_policy\s+=\s+'\w+'," ""))]]
        (println "Attaching table" table)
        (c/exec! "rm" file)
        (ch/exec! conn query))
      (println "Restore completed")

      (println (str "Merge DB '" temp-db "' to '" dest-db "'"))
      (doseq [[table parts] (ch/fetch-partitions conn :db temp-db)]
        (when-not (contains? dest-tables table)
          (println "Creating" (str dest-db "." table))
          (ch/exec! conn
                    (str/replace-first
                     (->> table (str temp-db ".") (ch/show-table conn))
                     #"TABLE\s+\w+\."
                     (str "TABLE " dest-db "."))))
        (doseq [part parts]
          (println "Moving" table part)
          (try
            (ch/exec! conn (str
                          "ALTER TABLE " temp-db "." table
                          " MOVE PARTITION " part
                          " TO TABLE " dest-db "." table))
            (catch Exception e (println (.getMessage e))))
          (println (str "Try to optimize table " dest-db "." table) part)
          (ch/exec! conn (str
                        "OPTIMIZE TABLE " dest-db "." table
                        " PARTITION " part
                        " FINAL"))))
      (println (str "Remove database '" temp-db "'"))
      (drop-db! conn temp-db)
      (println "Completed")
      (catch Exception e
        (println "\nException:" (.getMessage e)))))
  (c/exit!))

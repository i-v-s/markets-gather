(ns gather.backup
  (:require [clojure.string :as str]
            [house.ch       :as ch]
            [gather.common  :as c]
            [clojure.java.shell :as sh]))

(defn partition-test
  [p] (cond
    (nil? p) any?
    (str/includes? p "-")
      (let [[a b] (map c/to-uint (str/split p #"-"))]
        (fn [v] (c/within a b (c/to-uint v))))
    :else
      (partial = p)
    ))

(defn pack-target!
  [path target result]
  (let [cmd ["tar" "-rvf" result target :dir path]
        {exit :exit err :err} (apply sh/sh cmd)]
    (when (not= exit 0)
      (apply println "Command was:" cmd)
      (throw (Exception. err)))))

(defn store-root
  [p]
  (let [g (re-find #"^(.+/)store/\w+/[\w\-]+/$" p)]
    (assert g (str "Unable to parse store path from" p))
    (second g)))

(defn freeze
  ([conn db table] (freeze conn db table nil))
  ([conn db table part]
   (println "Freezing" table (or part "FULL"))
   (let [name "backup"]
     (ch/exec! conn (c/str-some
                     "ALTER TABLE " db "." table
                     " FREEZE"
                     (when part
                       (str " PARTITION " part))
                     (when name
                       (str " WITH NAME '" name "'"))))))
  ([conn db table part & parts]
   (->> parts (cons part) (map (partial freeze conn db table)) doall)))

(defn -main
  "Start with params"
  [db-url [wc partitions]]
  (println "Using DB URL:" db-url)
  (let [db (->> db-url (re-find #"/(\w+)$") second)]
    (assert db "Unable to parse DB name")
    (let [wcs (map c/make-wc (str/split wc #","))
          checker (c/some-fn-map (partial partial re-find) wcs)
          p-test (partition-test partitions)
          conn (ch/connect db-url)
          tabs (ch/fetch-partitions conn :db db)
          f-tabs (select-keys tabs (vec (distinct (filter checker (keys tabs)))))
          storage (select-keys (ch/fetch-storage conn) (keys f-tabs))
          storage-roots (->> storage vals (apply concat) (map store-root) distinct)
          shadows (map #(str % "shadow/backup/") storage-roots)
          result (-> "pwd" sh/sh :out str/trim (str "/backup" (or partitions "") ".tar"))]
      (println "Wildcards used:" (c/comma-join wcs))
      (println "DB to backup:" db)
      (if (empty? f-tabs)
        (println "No tables to backup.")
        (do
          (println "Tables to backup:" (->> f-tabs keys c/comma-join))
          (println "Storage path(s):" (c/comma-join storage-roots))
          (println "Shadow path(s):" (c/comma-join (map #(if (c/exists? %) (str % " - EXISTS!") %) shadows)))
          (println "Are you sure? (y/n)")
          (when (= (read-line) "y")
            (try
              (apply c/remove-dir! (filter c/exists? shadows))
              (doseq [[table parts] f-tabs]
                (if partitions
                  (apply freeze conn db table (filter p-test parts))
                  (freeze conn db table)))
              (println "Pack to" result)
              (doseq [[table storages] storage
                      :let [query (ch/show-table conn (str db "." table))]]
                (println "Packing" table "from" (c/comma-join storages))
                (doseq [path storages
                        :let [[g r t] (re-find #"^(.+/)store/(\w+/[\w\-]+/)$" path)
                              store (str r "shadow/backup/store/")
                              source (str store t)
                              temp (str store table)]
                        :when (c/exists? source)]
                  (assert g (str "Path parsing error: " path))
                  (c/exec! "mv" source temp)
                  (spit (str temp "/create.sql") query) ; TODO!
                  (pack-target! store table result)))
              (println "Remove shadows")
              (apply c/remove-dir! (filter c/exists? shadows))
              (println "Completed")
              (catch Exception e
                (println "\nException:" (.getMessage e)))))
          (c/exit!))))))

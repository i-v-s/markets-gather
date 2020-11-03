(ns gather.backup
  (:require
    [clojure.string :as str]
    [gather.ch :as ch]
    [gather.common :as c]
    [clojure.java.shell :as sh]
    [clojure.java.io :as io]))

(defn partition-test
  [p] (cond
    (nil? p)
      (fn [x] true)
    (str/includes? p "-")
      (let [[a b] (map c/to-uint (str/split p #"-"))]
        (fn [v] (c/within a b (c/to-uint v))))
    :else
      (partial = p)
    ))

(def shadow-dir "/var/lib/clickhouse/shadow")
(def meta-dir "/var/lib/clickhouse/metadata")

(defn list-files [sd] (file-seq sd))

(defn relative-split [base]
  (let [base (str/split (str base) #"/")]
    (fn [item]
      (subvec (str/split (str item) #"/") (count base)))))

(defn list-shadow-tables
  [sd]
  (for [
    rp (map (relative-split sd) (file-seq sd))
    :let [[n data db table & parts] rp]
    :when (and db table (empty? parts))
    ]
    [table (str sd "/" (str/join "/" (subvec rp 0 3)))]))

(defn pack-target!
  [path target result]
  ;(println "tar" "-rvf" result table :dir path)
  (let [{exit :exit err :err} (sh/sh "tar" "-rvf" result target :dir path)]
    (if (not= exit 0) (throw (Exception. err)))))

(defn pack-shadow!
  [sd result]
  (doseq [[table path] (gather.backup/list-shadow-tables (io/file sd))]
    (pack-target! path table result)))

(defn clean-dir
  [dir]
  (doseq [name (.list (io/file dir))]
    (sh/sh "rm" "-r" (str dir "/" name))))

(defn -main
  "Start with params"
  [db-url & [wc partitions]]
  (let [
      wcs (str/split (or wc "*") #",")
      p-test (partition-test partitions)
      st (ch/connect-st db-url)
      tabs (ch/fetch-partitions st :db "fx")
      f-tabs (select-keys tabs (vec (distinct (mapcat
        (fn [wc] (filter (c/wc-test wc) (keys tabs)))
        wcs))))
      result (-> "pwd" sh/sh :out str/trim (str "/backup" (or partitions "") ".tar"))
      tmp-dir "/tmp"
      meta-dir (str tmp-dir "/metadata")
    ]
    ;(println "Wildcards:" args)
    ;(println "Selected tables:" (keys f-tabs))
    (try
      (io/make-parents (str meta-dir "/x"))
      (clean-dir meta-dir)
      (doseq [[tab parts] f-tabs part (filter p-test parts)]
        (println "Freezing" tab part)
        (ch/exec-query! st (str "ALTER TABLE fx." tab " FREEZE PARTITION " part))
        (spit (str meta-dir "/" tab ".sql") (ch/show-table st (str "fx." tab)))
        )
      (println "Pack to" result)
      (pack-shadow! shadow-dir result)
      (println "Clean shadow")
      (clean-dir shadow-dir)
      (println "Pack metadata")
      (pack-target! tmp-dir "metadata" result)
      (println "Completed")
    (catch Exception e
      (println "\nException:" (.getMessage e))))))

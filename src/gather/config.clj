(ns gather.config
  (:require
    [clojure.java.io :as io]
    [clojure.data.json :as json]
    [clojure.walk :as w]
  ))

(defn exists?
  "Check if file exists"
  [file-name]
  (-> file-name io/as-file .exists))

(defn must
  [value pred message]
  (if (pred value)
    value
    (throw (Exception. (str message ": " value)))
  ))

(defn load-config
  "Reads and parse config.json data"
  [& [file-name]]
  (->
    (if (some? file-name)
      file-name
      (first (filter exists? [
        "config.json"
        "config.default.json"
      ]))
    )
    (must exists? "Config file not exists")
    slurp
    json/read-str
    w/keywordize-keys
  ))

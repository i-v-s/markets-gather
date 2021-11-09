(ns gather.config
  (:require
    [clojure.data.json :as json]
    [clojure.walk :as w]
  ))

(defn load-config
  "Reads and parse config.json data"
  [& file-name]
  (w/keywordize-keys (json/read-str (slurp (or file-name "config.json"))))
  )

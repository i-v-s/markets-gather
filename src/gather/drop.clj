(ns gather.drop
  (:require
    [gather.ch :as ch]))

(defn -main
  "Start with params"
  [db-url args]
  (let [
    st (ch/connect-st db-url)
    ]
    (println args)
    (ch/exec-query! st "USE fx")
    (println (ch/exec-query! st "SHOW TABLES"))
    ))

(ns gather.sql
  (:require
   [clojure.string :as str]
   [gather.common :as c]))

(defrecord Query [fields sql])

(def query? (partial instance? Query))

(defn q-str
  [& args]
  (apply str
         (for [a args]
           (if (instance? Query a)
             (str "(" (:sql a) ")")
             a))))

(defn parse-fields
  [fields]
  (loop [fs fields result []]
    (if (not-empty fs)
      (let [[e k n] fs]
        (assert (string? e))
        (if (and (= :as k) (string? n))
          (recur (drop 3 fs) (conj result n e))
          (recur (drop 1 fs) (conj result e nil))))
      (apply array-map result))))

(def joins
  {:inner-join "INNER JOIN"
   :left-join "LEFT JOIN"
   :right-join "RIGHT JOIN"})

(defn prepare-from-fn
  [items fields sql]
  (let [[e k n] items]
    (if (keyword? e)
      (if (= e :on)
        [(drop 2 items) fields (str sql " ON " k)]
        [(drop 1 items) fields (str sql " " (e joins) " ")])
      (let [[f s] (if (string? e)
                    [[] e]
                    [(:fields e) (str "(" (:sql e) ")")])]
        (if (= k :as)
          [(drop 3 items) (concat fields (map (partial str n ".") f)) (str sql s " AS " n)]
          [(drop 1 items) (concat fields f) (str sql s)])))))

(defn prepare-from
  [args _with]
  (loop [data [(if (vector? args) args [args]) [] ""]]
    (if (not-empty (first data))
      (recur (apply prepare-from-fn data))
      (Query. (second data) (last data)))))

(defn select
  [fields & {:keys [with from where group-by order-by]}]
  (let [from' (prepare-from from with)
        parsed (parse-fields (or fields (:fields from')))]
    (Query.
     (keys parsed)
     (c/str-some
      (when with
        (str "WITH "
             (c/comma-join
              (for [[k v] with]
                (q-str k " AS " v)))
             "\n"))
      "SELECT " (c/comma-join
                 (for [[k v] parsed]
                   (if v
                     (str v " AS " k)
                     k)))
      "\nFROM " (:sql from')
      (when where
        (str "\nWHERE " where))
      (when group-by
        (str "\nGROUP BY " group-by))
      (when order-by
        (str "\nORDER BY " order-by))))))

(defn union-all
  [& qs]
  (Query.
   (:fields (first qs))
   (str/join "\nUNION ALL\n" (map :sql qs))))

(defn where
  [w q]
  (select nil :from q :where w))

(defn order-by
  [ob q]
  (select nil :from q :order-by ob))

(defn enumerate
  ([num-col query]
   (select (concat ["rowNumberInAllBlocks()" :as num-col] (:fields query))
           :from query))
  ([query]
   (enumerate "num" query)))

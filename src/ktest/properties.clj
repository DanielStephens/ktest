(ns ktest.properties
  (:import [java.util Properties]))

(defn properties [p]
  (reduce-kv
    (fn [p k v]
      (doto p
        (.setProperty (name k) v)))
    (Properties.)
    p))

(ns ktest.core
  (:require [ktest.processor :as p]
            [ktest.stores :as stores])
  (:import [java.util Random]))

(defn default-opts []
  {:recurse true
   :recursion-limit 1000
   :partition (fn [k] (vec k))
   :seed (.nextLong (Random.))})

(defn driver
  [opts & name-topology-supplier-pairs]
  (when-not (even? (count name-topology-supplier-pairs))
    (throw (ex-info "Pairs of topologies and their config maps are expected" {})))
  (p/processor (merge (default-opts)
                      opts)
               (partition 2 name-topology-supplier-pairs)
               0))

(defn pipe [driver topic message]
  (p/pipe-inputs driver {topic [message]}))

(defn advance-time [driver advance-millis]
  (p/advance-time driver advance-millis))

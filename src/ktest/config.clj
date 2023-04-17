(ns ktest.config
  (:require [ktest.stores :as stores])
  (:import (java.util
            Random)))

(defn default-topology-mutator
  [topology _opts]
  (-> topology
      stores/share-global-stores
      stores/mutate-to-fast-stores))

(defn default-partition-strategy
  [_topic {:keys [key] :as msg}]
  (vec (str key)))

(defn default-opts
  []
  {:key-serde nil
   :value-serde nil
   :partition default-partition-strategy

   :recurse true
   :recursion-limit 1000

   :seed (.nextLong (Random.))
   :shuffle false

   :topo-mutator default-topology-mutator
   :initial-ms 0})

(defn mk-opts
  [own-opts]
  (merge (default-opts)
         own-opts))

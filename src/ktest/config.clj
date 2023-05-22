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
  [topic {:keys [key] :as msg} {:keys [key-serde]}]
  (let [topic-name (if (string? topic) topic (:topic-name topic))]
    (->> key
         (.serialize (.serializer key-serde) topic-name)
         (.deserialize (.deserializer key-serde) topic-name))))

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

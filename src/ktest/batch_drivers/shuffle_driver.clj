(ns ktest.batch-drivers.shuffle-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.utils :refer :all])
  (:import (java.util
            ArrayList
            Collection
            Collections
            Random)))

(defn- message-partition
  [opts topic message]
  ((:partition opts) topic message))

(defn- shuffle-with-random
  [^Collection coll ^Random random]
  (let [al (ArrayList. coll)]
    (Collections/shuffle al random)
    (vec al)))

(defn- shuffle-with-maintained-partitions
  [opts messages ^Random random]
  (let [enriched-msgs (map-indexed (fn [i msg]
                                     (assoc msg
                                            :partition (message-partition opts (:topic msg) msg)
                                            :index i))
                                   messages)
        partitions (group-by (juxt :topic :partition) enriched-msgs)

        fully-shuffled-msgs (shuffle-with-random enriched-msgs random)
        shuffled-partitions (group-by (juxt :topic :partition) fully-shuffled-msgs)

        replacement-map (->> (merge-with zipmap
                                         shuffled-partitions
                                         partitions)
                             vals
                             (apply merge))
        with-reordered-partitions (map replacement-map fully-shuffled-msgs)]
    (->> with-reordered-partitions
         (map #(dissoc % :partition :index)))))

(defrecord ShuffleDriver
  [opts delegate-batch-driver random]

  BatchDriver

  (pipe-inputs
    [_ messages]
    (pipe-inputs delegate-batch-driver
                 (shuffle-with-maintained-partitions opts messages random)))


  (advance-time
    [_ advance-millis]
    (advance-time delegate-batch-driver advance-millis))


  (current-time
    [_]
    (current-time delegate-batch-driver))


  (close [_] (close delegate-batch-driver)))

(defn batch-driver
  "Shuffles the order messages are consumed in, while maintaining order within
  a partition of a topic"
  [batch-driver {:keys [seed] :as opts}]
  {:pre [seed]}
  (->ShuffleDriver opts batch-driver (random seed)))

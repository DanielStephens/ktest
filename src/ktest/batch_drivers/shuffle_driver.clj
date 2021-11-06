(ns ktest.batch-drivers.shuffle-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.utils :refer :all])
  (:import [java.util Collection Random Collections ArrayList]))

(defn- shuffle-with-random
  [^Collection coll ^Random random]
  (let [al (ArrayList. coll)]
    (Collections/shuffle al random)
    (vec al)))

(defrecord ShuffleDriver [delegate-batch-driver random]
  BatchDriver
  (pipe-inputs [_ messages]
    (pipe-inputs delegate-batch-driver
                 (shuffle-with-random messages random)))
  (advance-time [_ advance-millis]
    (advance-time delegate-batch-driver advance-millis))
  (current-time [_]
    (current-time delegate-batch-driver))
  (close [_] (close delegate-batch-driver)))

(defn batch-driver
  [batch-driver {:keys [seed] :as _opts}]
  {:pre [seed]}
  (->ShuffleDriver batch-driver (random seed)))

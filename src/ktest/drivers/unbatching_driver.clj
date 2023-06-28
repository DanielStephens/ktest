(ns ktest.drivers.unbatching-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.protocols.driver :as d]
            [ktest.utils :refer :all]))

(defrecord UnBatchDriver
  [batch-driver]

  d/Driver

  (pipe-input
    [_ topic message]
    (pipe-inputs batch-driver [(assoc message :topic topic)]))


  (advance-time
    [_ advance-millis]
    (advance-time batch-driver advance-millis))


  (current-time
    [_]
    (current-time batch-driver))


  (stores-info
    [_]
    (stores-info batch-driver))


  (close [_] (close batch-driver)))

(defn driver
  [batched-driver]
  (->UnBatchDriver batched-driver))

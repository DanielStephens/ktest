(ns ktest.batch-drivers.batching-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.protocols.driver :as d]
            [ktest.utils :refer :all]))

(defrecord BatchUpDriver [driver]
  BatchDriver
  (pipe-inputs [_ messages]
    (->> messages
         (map (fn [m] (d/pipe-input driver (:topic m) m)))
         (munge-outputs)))
  (advance-time [_ advance-millis]
    (d/advance-time driver advance-millis))
  (current-time [_]
    (d/current-time driver))
  (close [_] (d/close driver)))

(defn batch-driver
  [driver]
  (->BatchUpDriver driver))

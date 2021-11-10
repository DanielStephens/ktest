(ns ktest.drivers.serde-driver
  (:require [ktest.protocols.driver :refer :all]
            [ktest.serde :refer :all]))

(defrecord ApplyingSerdeDriver [opts driver]
  Driver
  (pipe-input [_ topic message]
    (deserialise-output opts
                        (pipe-input driver topic (serialise opts topic message))))
  (advance-time [_ advance-millis]
    (deserialise-output opts (advance-time driver advance-millis)))
  (current-time [_]
    (current-time driver))
  (close [_] (close driver)))

(defn driver
  [driver opts]
  (->ApplyingSerdeDriver opts driver))

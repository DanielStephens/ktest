(ns ktest.batch-drivers.clean-output-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.utils :refer :all]))

(defn clean-msg
  [msg]
  (select-keys msg [:key :value]))

(defn clean-output
  [output]
  (->> output
       (map (fn [[topic msgs]] [topic (map clean-msg msgs)]))
       (into {})))

(defrecord CleaningBatchDriver
  [batch-driver]

  BatchDriver

  (pipe-inputs
    [_ messages]
    (clean-output (pipe-inputs batch-driver messages)))


  (advance-time
    [_ advance-millis]
    (clean-output (advance-time batch-driver advance-millis)))


  (current-time
    [_]
    (current-time batch-driver))


  (close [_] (close batch-driver)))

(defn batch-driver
  "Various drivers append extra information to message maps so rather than only
  containing a key and value, they might also contain the topic and information
  about repartitions.
  This driver cleans up any such information ensuring only the key and value are returned in a message map."
  [batch-driver]
  (->CleaningBatchDriver batch-driver))

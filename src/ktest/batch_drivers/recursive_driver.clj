(ns ktest.batch-drivers.recursive-driver
  (:require [ktest.protocols.batch-driver :refer :all]
            [ktest.utils :refer :all]))

(defn- recursively-pipe
  [depth {:keys [recursion-limit] :as opts}
   delegate-batch-driver inputs output]
  (if (> depth recursion-limit)
    (throw (ex-info "still more recursion necessary but safety limit has been reached." {}))
    (let [more-output (pipe-inputs delegate-batch-driver inputs)
          next-input (->> more-output
                          (mapcat (fn [[topic ms]] (map #(assoc % :topic topic) ms))))
          combined-output (munge-outputs [output more-output])]
      (if (seq next-input)
        (recur (inc depth) opts delegate-batch-driver
               next-input combined-output)
        combined-output))))

(defrecord RecursionDriver [batch-driver opts]
  BatchDriver
  (pipe-inputs [_ messages]
    (recursively-pipe 0 opts batch-driver messages {}))
  (advance-time [_ advance-millis]
    (let [initial-outputs (advance-time batch-driver advance-millis)
          inputs (->> initial-outputs
                      (mapcat (fn [[topic msgs]]
                                (map (fn [m] (assoc m :topic topic)) msgs))))]
      (recursively-pipe 1
                        opts batch-driver
                        inputs
                        initial-outputs)))
  (current-time [_]
    (current-time batch-driver))
  (close [_] (close batch-driver)))

(defn batch-driver
  [batch-driver opts]
  (->RecursionDriver batch-driver opts))

(ns ktest.drivers.completing-internals-driver
  (:require [ktest.protocols.driver :refer :all]
            [ktest.utils :refer :all]))

(defn repartition-topic?
  [topic]
  (and (map? topic)
       (:repartition topic)))

(defn- split-output
  [output]
  (let [{repartitions true real false} (group-by (comp repartition-topic? key) output)]
    (cond-> {}
      (seq repartitions) (assoc :repartitions (into {} repartitions))
      (seq real) (assoc :real (into {} real)))))

(defn flatten-output
  [output]
  (->> output
       (mapcat (fn [[topic msgs]] (map #(assoc % :topic topic) msgs)))))

(defn- process-messages-to-completion
  [depth {:keys [recursion-limit] :as opts}
   driver inputs output]
  (if (> depth recursion-limit)
    (throw (ex-info "still more recursion necessary but safety limit has been reached." {}))
    (if-let [input (first inputs)]
      (let [{:keys [real repartitions]} (->> input
                                             (pipe-input driver (:topic input))
                                             (split-output))
            next-input (concat (rest inputs)
                               (flatten-output repartitions))
            combined-output (munge-outputs [output real])]
        (recur (inc depth) opts driver
               next-input combined-output))
      output)))

(defrecord CompletingInternalsDriver
  [driver opts]

  Driver

  (pipe-input
    [_ topic message]
    (let [initial-result (pipe-input driver topic message)
          {:keys [real repartitions]} (split-output initial-result)]
      (process-messages-to-completion 1 opts driver
                                      (flatten-output repartitions)
                                      real)))


  (advance-time
    [_ advance-millis]
    (let [initial-result (advance-time driver advance-millis)
          {:keys [real repartitions]} (split-output initial-result)]
      (process-messages-to-completion 1 opts driver
                                      (flatten-output repartitions)
                                      real)))


  (stores-info
    [_]
    (stores-info driver))


  (current-time
    [_]
    (current-time driver))


  (close [_] (close driver)))

(defn driver
  "This driver collects the messages that appear on the internal repartition
  topics and ensures that we only output the final result by pushing the
  repartition messages back into the topology and filtering them out of the
  result."
  [driver opts]
  (->CompletingInternalsDriver driver opts))

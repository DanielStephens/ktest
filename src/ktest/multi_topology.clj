(ns ktest.multi-topology
  (:require [ktest.driver :refer :all]
            [ktest.topology-group :as tg]))

(defn- munge-outputs
  [topic-output-maps]
  (apply merge-with concat topic-output-maps))

(defrecord MultiTopologyDriver
  [opts drivers]
  Driver
  (pipe-raw-input [_ topic message]
    (->> drivers
         (filter (fn [d] ((:sources d) topic)))
         (map (fn [d] (pipe-raw-input d topic message)))
         (munge-outputs)))
  (advance-time [_ advance-millis]
    (->> drivers
         (map (fn [d] (advance-time d advance-millis)))
         (munge-outputs)))
  (close [_] (when-let [errors (->> drivers
                                    (map #(try (do (close %) nil)
                                               (catch Exception e e)))
                                    doall
                                    (filter some?)
                                    seq)]
               (throw (ex-info "Closing topologies failed."
                               {:errors errors})))))

(defn multi-topology-driver
  [id opts name-topology-supplier-pairs initial-epoch]
  (let [drivers (->> name-topology-supplier-pairs
                     (map (fn [[app-id topology-supplier]] (tg/topology-group-driver opts
                                                                                     topology-supplier
                                                                                     (str app-id "-" id)
                                                                                     initial-epoch)))
                     doall)]
    (->MultiTopologyDriver opts drivers)))


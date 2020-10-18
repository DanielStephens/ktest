(ns ktest.topology-group
  (:require [ktest.driver :refer :all]
            [ktest.topology :as t]
            [clojure.string :as str]))

(defn- message-partition [opts key]
  ((:partition opts) key))

(defn- sanitise-app-id [app-id]
  (str/replace app-id #"[^a-zA-Z0-9._-]" "_"))

(defn- partitioned-application-id [root-application-id partition]
  (sanitise-app-id (str root-application-id "-" partition)))

(defn- repartition-topic [root-application-id partition root-topic]
  (str (partitioned-application-id root-application-id partition) "-" root-topic))

(defn- driver! [state partition supplier]
  (let [k [:drivers partition]
        ;; using a delay avoids us doing lots of work repeatedly making the
        ;; test driver if our atom gets conflicts
        new-topology (delay (supplier partition))
        s (swap! state (fn [s] (if (get-in s k) s (assoc-in s k @new-topology))))]
    (get-in s k)))

(defn- drivers [state]
  (->> (:drivers @state)
       vals
       seq))

(defn- munge-outputs
  [topic-output-maps]
  (apply merge-with concat topic-output-maps))

(defn- flatten-repartitions [opts root-application-id repartitions]
  (->> repartitions
       (mapcat (fn [[root-topic ms]] (map #(assoc % :root-topic root-topic) ms)))
       (map (fn [m] (assoc m :partition (message-partition opts (:key m)))))
       (map (fn [m] (assoc m :topic (repartition-topic root-application-id
                                                       (:partition m)
                                                       (:root-topic m)))))
       seq))

(defn- handle-output! [opts state root-application-id supplier output inputs]
  (if-let [ins (seq inputs)]
    (let [m (first ins)
          d (driver! state (:partition m) supplier)
          more-output (pipe-raw-input d (:topic m) m)]
      (recur opts state root-application-id supplier
             (munge-outputs [output (:output more-output)])
             (flatten-repartitions opts root-application-id (:repartitions more-output))))
    output))

(defrecord TopologyGroupDriver
  [opts state root-application-id sources sinks repartition-topic? supplier]
  Driver
  (pipe-raw-input [_ topic message]
    (handle-output! opts state root-application-id supplier
                    {}
                    [(assoc message
                       :topic topic
                       :partition (message-partition opts (:key message)))]))
  (advance-time [_ advance-millis]
    (swap! state update :epoch + advance-millis)
    (let [os (->> (drivers state)
                  (map #(advance-time % advance-millis))
                  doall)
          outputs (->> os
                       (map :output)
                       (munge-outputs))
          repartitions (->> os
                            (map :repartitions)
                            (munge-outputs)
                            (flatten-repartitions opts root-application-id))]
      (handle-output! opts state root-application-id supplier
                      outputs repartitions)))
  (close [_] (when-let [errors (->> (drivers state)
                                    (map #(try (do (close %) nil)
                                               (catch Exception e e)))
                                    doall
                                    (filter some?)
                                    seq)]
               (throw (ex-info "Closing topologies failed."
                               {:topology root-application-id
                                :errors errors})))))

(defn topology-group-driver
  [opts topology-supplier application-id initial-epoch]
  (let [topology (topology-supplier)
        state (atom {:epoch initial-epoch})
        supplier #(t/topology-driver opts topology
                                     (partitioned-application-id application-id %)
                                     (:epoch @state))
        {:keys [sources
                sinks
                repartition-topic?]} (driver! state :default supplier)]
    (->TopologyGroupDriver opts state application-id sources sinks repartition-topic? supplier)))

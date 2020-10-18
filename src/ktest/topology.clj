(ns ktest.topology
  (:require [ktest.driver :refer :all]
            [clojure.string :as str]
            [ktest.internal.interop :as i])
  (:import [org.apache.kafka.streams TopologyTestDriver TopologyInternalsAccessor]
           [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.common.record TimestampType]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.streams.processor.internals StreamTask]))

(defn- consumer-record [state topic {:keys [key value]}]
  (ConsumerRecord. topic
                   0 0
                   (:epoch @state) TimestampType/CREATE_TIME
                   -1
                   (count (vec key)) (count (vec value))
                   key value))

(defn- raw-repartition-topic
  [application-id topic]
  (if (str/starts-with? topic (str application-id "-"))
    (subs topic (inc (count application-id)))
    (throw (ex-info "This method should only be called with a topologies repartition topics" {}))))

(defn- default-capture [application-id allow-first? source? repartition-topic? repartitions]
  (let [first-time? (atom allow-first?)]
    (fn [^StreamTask delegate ^TopicPartition topic-partition message]
      (let [[first-time? _] (reset-vals! first-time? false)
            topic (.topic topic-partition)]
        (cond
          ;; the first time we see something, it's the thing we just manually
          ;; sent in, so let it through!
          first-time? (.addRecords delegate topic-partition [message])

          ;; if it's a repartition topic we should make sure we direct this to
          ;; the same topology but on the correct partition so capture it
          (repartition-topic? topic) (swap! repartitions
                                            conj
                                            {(raw-repartition-topic application-id topic)
                                             [{:key (.key message)
                                               :value (.value message)}]})

          ;; any time we hit a source except the first time, the topology
          ;; driver will also say this is an output so don't do anything
          ;; otherwise we'll collect it twice
          (source? topic) nil

          ;; else it is an internal topic that doesn't cause a repartition, so
          ;; let it go through the current topology
          :else (.addRecords delegate topic-partition [message]))))))

(defn- read-exhaustively [^TopologyTestDriver driver sink]
  (take-while some? (repeatedly #(.readOutput driver sink))))

(defn- collect-outputs [^TopologyTestDriver driver sinks]
  (->> sinks
       (mapcat (partial read-exhaustively driver))
       (map #(do {(.topic %) [{:key (.key %) :value (.value %)}]}))))

(defn- munge-outputs
  [topic-output-maps]
  (apply merge-with concat topic-output-maps))

(defn- form-output
  [^TopologyTestDriver driver sinks repartitions]
  (let [main-output (munge-outputs (collect-outputs driver sinks))
        repartition-output (munge-outputs repartitions)]
    {:output main-output
     :repartitions repartition-output}))

(defrecord TopologyDriver
  [state ^TopologyTestDriver driver application-id sources sinks repartition-topic?]
  Driver
  (pipe-raw-input [_ topic message]
    (let [repartitions (atom [])]
      (swap! state assoc :capture (default-capture application-id
                                                   true
                                                   sources
                                                   repartition-topic?
                                                   repartitions))
      (.pipeInput driver
                  (consumer-record state topic message))
      (form-output driver sinks @repartitions)))
  (advance-time [_ advance-millis]
    (swap! state update :epoch + advance-millis)
    (let [repartitions (atom [])]
      (swap! state assoc :capture (default-capture application-id
                                                   false
                                                   sources
                                                   repartition-topic?
                                                   repartitions))
      (.advanceWallClockTime driver advance-millis)
      (form-output driver sinks @repartitions)))
  (close [_] (.close driver)))

(defn topology-driver [_opts topology application-id initial-epoch]
  (let [state (atom {:epoch initial-epoch})

        config {"application.id" application-id
                "bootstrap.servers" ""}
        driver (i/test-driver topology config initial-epoch
                              (fn [delegate topic-partition record]
                                ((:capture @state) delegate topic-partition record)))

        p (TopologyInternalsAccessor/processorTopology driver)
        gp (TopologyInternalsAccessor/globalProcessorTopology driver)

        repartition-topic? (fn [topic]
                             (or (when p (TopologyInternalsAccessor/isRepartitionTopic p topic))
                                 (when gp (TopologyInternalsAccessor/isRepartitionTopic gp topic))))

        p-sources (when p (.sourceTopics p))
        gp-sources (when gp (.sourceTopics gp))
        sources (->> (concat p-sources gp-sources)
                     (remove repartition-topic?)
                     (set))

        p-sinks (when p (.sinkTopics p))
        gp-sinks (when gp (.sinkTopics gp))
        sinks (->> (concat p-sinks gp-sinks)
                   (remove repartition-topic?)
                   (set))]
    (->TopologyDriver state driver application-id sources sinks repartition-topic?)))
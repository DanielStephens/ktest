(ns ktest.drivers.topology-driver
  (:require [clojure.string :as str]
            [ktest.internal.interop :as i]
            [ktest.protocols.driver :refer :all]
            [ktest.utils :refer :all])
  (:import (java.time
            Duration
            Instant)
           (org.apache.kafka.common
            TopicPartition)
           (org.apache.kafka.streams
            TopologyInternalsAccessor
            TopologyTestDriver)
           (org.apache.kafka.streams.processor
            StateStore)
           (org.apache.kafka.streams.processor.internals
            StreamTask)
           (org.apache.kafka.streams.state
            ValueAndTimestamp)))

(defn- raw-repartition-topic
  [application-id topic]
  (if (str/starts-with? topic (str application-id "-"))
    (subs topic (inc (count application-id)))
    (throw (ex-info "This method should only be called with a topologies repartition topics" {}))))

(defn- default-capture
  [application-id allow-first? source? repartition-topic? repartitions]
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

(defn- read-exhaustively
  [^TopologyTestDriver driver sink opts]
  (->> (.createOutputTopic driver sink (.deserializer (:key-serde opts)) (.deserializer (:value-serde opts)))
       (.readRecordsToList)
       (map #(do {sink [{:key (.key %) :value (.value %)}]}))))

(defn- collect-outputs
  [^TopologyTestDriver driver sinks opts]
  (->> sinks
       (mapcat #(read-exhaustively driver % opts))))

(defn- deserialize-msg
  [opts topic msg]
  (let [key-deserilizer (.deserializer (:key-serde opts))
        value-deserilizer (.deserializer (:value-serde opts))]
    (-> msg
        (update :key #(.deserialize key-deserilizer topic %))
        (update :value #(.deserialize value-deserilizer topic %)))))

(defn- form-output
  [^TopologyTestDriver driver root-application-id sinks repartitions opts]
  (let [main-output (munge-outputs (collect-outputs driver sinks opts))
        repartition-output (->> (munge-outputs repartitions)
                                (map (fn [[topic msgs]]
                                       [{:repartition true
                                         :application-id root-application-id
                                         :topic-name topic}
                                        (map (partial deserialize-msg opts topic) msgs)]))
                                (into {}))]
    (munge-outputs [main-output repartition-output])))

(defn partitioned-application-id
  [application-id partition-id]
  (str application-id "-" partition-id))

(defn resolve-topic
  [root-application-id application-id sources topic]
  (cond
    (and (map? topic)
         (:repartition topic)
         (= (:application-id topic) root-application-id))
    (str application-id
         "-"
         (:topic-name topic))

    (contains? sources topic) topic

    :else nil))

(defn extra-info-from-store
  [state-store]
  (->> (.all state-store)
       (iterator-seq)
       (reduce (fn [result key-value]
                 (let [v (.value key-value)
                       v (if (instance? ValueAndTimestamp v)
                           (.value v)
                           v)]
                   (assoc result (.key key-value) v))) {})))

(defrecord TopologyDriver
  [opts state ^TopologyTestDriver driver
   root-application-id application-id partition-id
   sources sinks repartition-topic? key-serde value-serde]

  Driver

  (pipe-input
    [_ topic message]
    (when-let [resolved-topic (resolve-topic root-application-id application-id sources
                                             topic)]
      (let [repartitions (atom [])
            input-topic (.createInputTopic driver resolved-topic (.serializer key-serde) (.serializer value-serde))]
        (swap! state assoc :capture (default-capture application-id
                                      true
                                      sources
                                      repartition-topic?
                                      repartitions))
        (.pipeInput input-topic (:key message) (:value message))
        (form-output driver root-application-id sinks @repartitions opts))))


  (advance-time
    [_ advance-millis]
    (swap! state update :epoch + advance-millis)
    (let [repartitions (atom [])]
      (swap! state assoc :capture (default-capture application-id
                                    false
                                    sources
                                    repartition-topic?
                                    repartitions))
      (.advanceWallClockTime driver (Duration/ofMillis advance-millis))
      (form-output driver root-application-id sinks @repartitions opts)))


  (stores-info
    [_]
    (->> (.getAllStateStores driver)
         (map (fn [[n ^StateStore state-store]]
                (when (nil? state-store) (throw (ex-info "State store is nil, this is likely due to it being added, but not used, in the topology"
                                                         {:store-name n})))
                [n {partition-id (extra-info-from-store state-store)}]))
         (into {})))


  (current-time [_] (:epoch @state))


  (close [_] (.close driver)))

(defn- topo-and-config
  [topology-supplier]
  (let [r (topology-supplier)]
    (if (map? r)
      r
      {:topology r
       :config {}})))

(defn driver
  [root-application-id partition-id topology-supplier opts]
  (let [initial-epoch (Instant/ofEpochMilli (:initial-ms opts))
        state (atom {:epoch (:initial-ms opts)})

        {:keys [topology config]} (topo-and-config topology-supplier)
        application-id (partitioned-application-id root-application-id partition-id)
        adjusted-config (merge {"bootstrap.servers" ""}
                               (into {} config)
                               {"application.id" application-id})
        mutated-topology ((:topo-mutator opts) topology opts)
        driver (i/test-driver mutated-topology adjusted-config initial-epoch
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
    (->TopologyDriver opts state driver
                      root-application-id application-id
                      partition-id sources sinks repartition-topic?
                      (:key-serde opts) (:value-serde opts))))

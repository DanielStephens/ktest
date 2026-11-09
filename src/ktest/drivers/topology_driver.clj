(ns ktest.drivers.topology-driver
  (:require [clojure.string :as str]
            [ktest.internal.interop :as i]
            [ktest.protocols.driver :refer :all]
            [ktest.utils :refer :all])
  (:import (java.io
            File)
           (java.nio.charset
            StandardCharsets)
           (java.nio.file
            Files)
           (java.nio.file.attribute
            FileAttribute)
           (java.time
            Duration
            Instant)
           (org.apache.kafka.clients.consumer
            ConsumerRecord)
           (org.apache.kafka.common
            TopicPartition)
           (org.apache.kafka.common.header
            Header)
           (org.apache.kafka.common.header.internals
            RecordHeaders)
           (org.apache.kafka.common.serialization
            Serde)
           (org.apache.kafka.common.utils
            Utils)
           (org.apache.kafka.streams
            KeyValue
            TopologyInternalsAccessor
            TopologyTestDriver)
           (org.apache.kafka.streams.processor
            StateStore)
           (org.apache.kafka.streams.processor.internals
            StreamTask)
           (org.apache.kafka.streams.state
            KeyValueIterator
            KeyValueStore
            ValueAndTimestamp)
           (org.apache.kafka.streams.test
            TestRecord)))

(defn- raw-repartition-topic
  [application-id topic]
  (if (str/starts-with? topic (str application-id "-"))
    (subs topic (inc (count application-id)))
    (throw (ex-info "This method should only be called with a topologies repartition topics" {}))))

(defn headers->map
  [headers]
  (reduce (fn [m ^Header h]
            (let [k (.key h)]
              (when (contains? m k)
                (binding [*out* *err*]
                  (println "WARN: duplicate Kafka header key:" k)))
              (assoc m k (when-let [v (.value h)] (String. v StandardCharsets/UTF_8)))))
          {}
          headers))

(defn map->headers
  [header-map]
  (when (not-empty header-map)
    (let [headers (RecordHeaders.)]
      (doseq [[k v] header-map]
        (.add headers ^String k (when v (.getBytes ^String v StandardCharsets/UTF_8))))
      headers)))

(defn- default-capture
  [application-id allow-first? source? repartition-topic? repartitions]
  (let [first-time? (atom allow-first?)]
    (fn [^StreamTask delegate ^TopicPartition topic-partition ^ConsumerRecord message]
      (let [[first-time? _] (reset-vals! first-time? false)
            topic (.topic topic-partition)]
        (cond
          first-time? (.addRecords delegate topic-partition [message])

          (repartition-topic? topic) (swap! repartitions
                                            conj
                                            {(raw-repartition-topic application-id topic)
                                             [{:key     (.key message)
                                               :value   (.value message)
                                               :headers (headers->map (.headers message))}]})

          (source? topic) nil

          :else (.addRecords delegate topic-partition [message]))))))

(defn- read-exhaustively
  [^TopologyTestDriver driver sink {:keys [^Serde key-serde ^Serde value-serde]}]
  (->> (.createOutputTopic driver sink (.deserializer key-serde) (.deserializer value-serde))
       (.readRecordsToList)
       (map (fn [^TestRecord record]
              (let [value (.value record)
                    hdrs (headers->map (.headers record))
                    msg {:key (.key record) :value value}]
                {sink [(if (seq hdrs)
                         (vary-meta msg assoc :kafka-headers hdrs)
                         msg)]})))))

(defn- collect-outputs
  [^TopologyTestDriver driver sinks opts]
  (->> sinks
       (mapcat #(read-exhaustively driver % opts))))

(defn- deserialize-msg
  [{:keys [^Serde key-serde ^Serde value-serde]} topic msg]
  (-> msg
      (update :key #(.deserialize (.deserializer key-serde) topic %))
      (update :value #(.deserialize (.deserializer value-serde) topic %))))

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
  [^KeyValueStore state-store]
  (->> (.all state-store)
       (iterator-seq)
       (reduce (fn [result ^KeyValue key-value]
                 (let [v (.value key-value)
                       v (if (instance? ValueAndTimestamp v)
                           (.value ^ValueAndTimestamp v)
                           v)]
                   (assoc result (.key key-value) v))) {})))

(defrecord TopologyDriver
  [opts state ^TopologyTestDriver driver
   root-application-id application-id partition-id
   sources sinks repartition-topic? ^Serde key-serde ^Serde value-serde]

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
        (if-let [hdrs (map->headers (:headers message))]
          (.pipeInput input-topic (TestRecord. (:key message) (:value message) ^org.apache.kafka.common.header.Headers hdrs))
          (.pipeInput input-topic (:key message) (:value message)))
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

;; Kafka's default state.dir depends only on the application id, so unclosed
;; drivers leak RocksDB state into later runs. Use a fresh dir per driver.
(def ^:private state-dir-root
  (delay
   (let [root (.toFile (Files/createTempDirectory "ktest-state-" (make-array FileAttribute 0)))]
     (.addShutdownHook (Runtime/getRuntime)
                       (Thread. ^Runnable (fn []
                                            (try
                                              (Utils/delete root)
                                              (catch Exception _)))))
     root)))

(defn- fresh-state-dir
  []
  (str (Files/createTempDirectory (.toPath ^File @state-dir-root) "d-" (make-array FileAttribute 0))))

(defn- topo-and-config
  [topology-supplier]
  (let [r (topology-supplier)]
    (if (map? r)
      r
      {:topology r
       :config {}})))

(defn ^ktest.protocols.driver.Driver driver
  [root-application-id partition-id topology-supplier opts]
  (let [initial-epoch (Instant/ofEpochMilli (:initial-ms opts))
        state (atom {:epoch (:initial-ms opts)})

        {:keys [topology config]} (topo-and-config topology-supplier)
        application-id (partitioned-application-id root-application-id partition-id)
        adjusted-config (merge {"bootstrap.servers" ""
                                "state.dir" (fresh-state-dir)}
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

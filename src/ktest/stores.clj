(ns ktest.stores
  (:import [org.apache.kafka.streams TopologyInternalsAccessor]
           [org.apache.kafka.streams.processor.internals InternalTopologyBuilder InternalTopologyBuilder$StateStoreFactory]
           [org.apache.kafka.streams.state.internals TimestampedKeyValueStoreBuilder AbstractStoreBuilder StoreAccessor ValueAndTimestampSerde ValueAndTimestampDeserializer KeyValueStoreBuilder WindowStoreBuilder TimestampedWindowStoreBuilder SessionStoreBuilder]
           [org.apache.kafka.streams.state Stores]
           [org.apache.kafka.common.serialization Serde Serdes]
           [java.time Duration]))

(def ^:private store-factory-users-field
  (let [f (.getDeclaredField InternalTopologyBuilder$StateStoreFactory "users")]
    (.setAccessible f true)
    f))

(def ^:private store-factory-builder-field
  (let [f (.getDeclaredField InternalTopologyBuilder$StateStoreFactory "builder")]
    (.setAccessible f true)
    f))

(defmulti find-known-alternative
          (fn [builder _store-name] (type builder)))

(defmethod find-known-alternative :default
  [builder store-name]
  (println "Store [" store-name "] was of an unhandled type [" (type builder) "] and could not be sped up")
  builder)

(defn key-serde [^AbstractStoreBuilder builder]
  (StoreAccessor/keySerde builder))

(defn value-serde [^AbstractStoreBuilder builder]
  (StoreAccessor/valueSerde builder))

(defn de-timestamp-serde [^ValueAndTimestampSerde serde]
  (StoreAccessor/deTimestampSerde serde))

(defmethod find-known-alternative KeyValueStoreBuilder
  [builder store-name]
  (Stores/keyValueStoreBuilder
    (Stores/inMemoryKeyValueStore store-name)
    (key-serde builder) (value-serde builder)))

(defmethod find-known-alternative TimestampedKeyValueStoreBuilder
  [builder store-name]
  (Stores/timestampedKeyValueStoreBuilder
    (Stores/inMemoryKeyValueStore store-name)
    (key-serde builder) (de-timestamp-serde (value-serde builder))))

(defmethod find-known-alternative SessionStoreBuilder
  [builder store-name]
  (Stores/sessionStoreBuilder
    (Stores/inMemorySessionStore store-name
                                 (Duration/ofMillis (.retentionPeriod builder)))
    (key-serde builder) (value-serde builder)))

;; can't be bothered to get window sizes currently

#_(defmethod find-known-alternative WindowStoreBuilder
  [builder store-name]
  (Stores/windowStoreBuilder
    (Stores/inMemoryKeyValueStore store-name)
    (key-serde builder) (value-serde builder)))

#_(defmethod find-known-alternative TimestampedWindowStoreBuilder
  [builder store-name]
  (Stores/timestampedWindowStoreBuilder
    (Stores/inMemoryWindowStore store-name)
    (key-serde builder) (value-serde builder)))

(defn alternative-store
  [store-name ^InternalTopologyBuilder$StateStoreFactory state-store-factory]
  (let [builder (.get store-factory-builder-field state-store-factory)
        users (.get store-factory-users-field state-store-factory)
        alt (find-known-alternative builder store-name)]
    {:name store-name
     :users (set users)
     :builder alt}))

(defn mutate-to-fast-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        stores (.getStateStores ^InternalTopologyBuilder i-builder)
        store-names (keys stores)]
    (doseq [store-name store-names
            :let [{:keys [users builder]} (alternative-store store-name (get stores store-name))]]
      (.addStateStore i-builder builder true (into-array String users)))
    topology))

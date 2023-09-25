(ns ktest.test-utils
  (:refer-clojure :exclude [map reduce])
  (:require [clojure.edn :as edn])
  (:import (java.nio.charset
            StandardCharsets)
           (java.time
            Duration)
           (org.apache.kafka.common.serialization
            Deserializer
            Serdes
            Serializer)
           (org.apache.kafka.streams
            KeyValue
            StreamsBuilder)
           (org.apache.kafka.streams.kstream
            Aggregator
            Consumed
            GlobalKTable
            Grouped
            Initializer
            Joined
            KGroupedStream
            KStream
            KTable
            KeyValueMapper
            Materialized
            Named
            Produced
            Reducer
            Stores
            Transformer
            TransformerSupplier
            ValueJoiner
            ValueMapper)
           (org.apache.kafka.streams.processor
            ProcessorContext
            PunctuationType
            Punctuator
            To)))

(def edn-serde
  (Serdes/serdeFrom
   (reify Serializer
     (serialize
       [_ _topic v]
       (.getBytes (prn-str v)
                  StandardCharsets/UTF_8)))
   (reify Deserializer
     (deserialize
       [_ _topic b]
       (edn/read-string (String. b StandardCharsets/UTF_8))))))

(def serde-config
  {:key-serde edn-serde
   :value-serde edn-serde})

(defn topic-config
  [topic-name]
  (assoc serde-config
         :topic-name topic-name))

(defn build-topology
  [builder]
  (.build ^StreamsBuilder builder))

(defn transformer
  [f]
  (fn []
    (let [s (atom {})]
      (reify Transformer
        (init [_ ctx] (swap! s assoc :ctx ctx))

        (transform [_ k v] (f (:ctx @s) k v))

        (close [_])))))

(defn punctuator
  [interval f]
  (fn []
    (reify Transformer
      (init
        [_ ctx]
        (.schedule ^ProcessorContext ctx
                   ^Duration interval PunctuationType/WALL_CLOCK_TIME
                   (reify Punctuator
                     (punctuate [_ epoch] (f ctx epoch)))))

      (transform [_ _ _])

      (close [_]))))

(defn streams-builder
  []
  (StreamsBuilder.))

(defn add-store
  [store]
  (.addStateStore (streams-builder) (.keyValueStoreBuilder (.persistentKeyValueStore store) nil nil)))

(defn ktable
  ([builder topic-config store-name]
   (.table ^StreamsBuilder builder
           ^String (:topic-name topic-config)
           ^Consumed (Consumed/with (:key-serde topic-config)
                                    (:value-serde topic-config))
           ^Materialized (-> (Materialized/as ^String store-name)
                             (.withKeySerde (:key-serde topic-config))
                             (.withValueSerde (:value-serde topic-config)))))
  ([builder topic-config]
   (ktable builder topic-config (:topic-name topic-config))))

(defn kstream
  [builder topic-config]
  (.stream ^StreamsBuilder builder
           ^String (:topic-name topic-config)
           ^Consumed (Consumed/with (:key-serde topic-config)
                                    (:value-serde topic-config))))

(defn select-key
  [stream select-key-fn]
  (.selectKey ^KStream stream
              (reify KeyValueMapper
                (apply [_ k v] (select-key-fn [k v])))))

(defn left-join
  ([stream kt joiner-fn
    stream-serde-config table-serde-config]
   (.leftJoin ^KStream stream
              ^KTable kt
              ^ValueJoiner (reify ValueJoiner
                             (apply [_ a b] (joiner-fn a b)))
              ^Joined (Joined/with (:key-serde stream-serde-config)
                                   (:value-serde stream-serde-config)
                                   (:value-serde table-serde-config))))
  ([stream kt joiner-fn]
   (.leftJoin ^KStream stream
              ^KTable kt
              ^ValueJoiner (reify ValueJoiner
                             (apply [_ a b] (joiner-fn a b))))))

(defn to
  [kstream topic-config]
  (.to ^KStream kstream
       ^String (:topic-name topic-config)
       ^Produced (Produced/with (:key-serde topic-config)
                                (:value-serde topic-config))))

(defn map
  [kstream map-fn]
  (.map ^KStream kstream
        ^KeyValueMapper (reify KeyValueMapper
                          (apply
                            [_ k v]
                            (let [[k' v'] (map-fn [k v])]
                              (KeyValue/pair k' v'))))))

(defn map-values
  [kstream map-values-fn]
  (.mapValues ^KStream kstream
              ^ValueMapper (reify ValueMapper
                             (apply [_ v] (map-values-fn v)))))

(defn through
  [kstream topic-config]
  (.through ^KStream kstream
            ^String (:topic-name topic-config)
            ^Produced (Produced/with (:key-serde topic-config)
                                     (:value-serde topic-config))))

(defn aggregate
  [grouped-stream initializer-fn aggregate-fn store-config]
  (.aggregate ^KGroupedStream grouped-stream
              ^Initializer (reify Initializer
                             (apply [_] (initializer-fn)))
              ^Aggregator (reify Aggregator
                            (apply [_ k v agg] (aggregate-fn agg [k v])))
              ^Materialized (-> (Materialized/as ^String (:topic-name store-config))
                                (.withKeySerde (:key-serde store-config))
                                (.withValueSerde (:value-serde store-config)))))

(defn group-by-key
  ([stream]
   (.groupByKey stream))
  ([stream serde-config]
   (.groupByKey stream (Grouped/with (:key-serde serde-config)
                                     (:value-serde serde-config)))))

(defn to-kstream
  [ktable]
  (.toStream ktable))

(defn transform
  ([stream transformer-supplier-fn stores]
   (.transform stream
               (reify TransformerSupplier
                 (get
                   [_]
                   (transformer-supplier-fn)))
               (into-array String stores)))
  ([stream transformer-supplier-fn]
   (.transform stream
               (reify TransformerSupplier
                 (get
                   [_]
                   (transformer-supplier-fn)))
               (into-array String []))))

(defn reduce
  [stream reduce-fn store-config]
  (.reduce ^KGroupedStream stream
           ^Reducer (reify Reducer
                      (apply [_ v1 v2] (reduce-fn v1 v2)))
           ^Named (Named/as (:topic-name store-config))
           ^Materialized (Materialized/with (:key-serde store-config)
                                            (:value-serde store-config))))

(defn global-ktable
  [builder table-config]
  (.globalTable ^StreamsBuilder builder
                ^String (:topic-name table-config)
                ^Consumed (Consumed/with (:key-serde table-config)
                                         (:value-serde table-config))))

(defn left-join-global
  [stream gkt key-fn joiner-fn]
  (.leftJoin ^KStream stream
             ^GlobalKTable gkt
             ^KeyValueMapper (reify KeyValueMapper
                               (apply [_ k v] (key-fn [k v])))
             ^ValueJoiner (reify ValueJoiner
                            (apply [_ a b] (joiner-fn a b)))))

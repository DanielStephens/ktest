(ns ktest.serde
  (:import (org.apache.kafka.common.serialization
            Deserializer
            Serde
            Serializer)))

(defn topic-name
  [topic]
  (if (map? topic)
    (:topic-name topic)
    topic))

(defn serialise
  [{:keys [^Serde key-serde ^Serde value-serde]} topic msg]
  (-> msg
      (update :key #(.serialize ^Serializer (.serializer key-serde) (topic-name topic) %))
      (update :value #(.serialize ^Serializer (.serializer value-serde) (topic-name topic) %))))

(defn deserialise
  [{:keys [^Serde key-serde ^Serde value-serde]} topic msg]
  (-> msg
      (update :key #(.deserialize ^Deserializer (.deserializer key-serde) (topic-name topic) %))
      (update :value #(.deserialize ^Deserializer (.deserializer value-serde) (topic-name topic) %))))

(defn deserialise-output
  [opts messages]
  (->> messages
       (map (fn [[topic ms]]
              [topic (mapv #(deserialise opts
                                         topic
                                         %) ms)]))
       (into {})))

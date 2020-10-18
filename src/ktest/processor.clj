(ns ktest.processor
  (:require [ktest.raw-processor :as rp])
  (:import [org.apache.kafka.common.serialization Deserializer Serde Serializer]))

(defprotocol ReadableBatchDriver
  (pipe-inputs [this messages])
  (advance-time [this advance-millis])
  (close [this]))

(defn- serialise
  [{:keys [^Serde key-serde ^Serde value-serde]} topic {:keys [key value]}]
  {:key (.serialize ^Serializer (.serializer key-serde) topic key)
   :value (.serialize ^Serializer (.serializer value-serde) topic value)})

(defn- serialise-input
  [opts messages]
  (->> messages
       (map (fn [m] (merge m (serialise opts (:topic m) m))))))

(defn- flatten-input
  [messages]
  (->> messages
       (mapcat (fn [[topic ms]] (map #(assoc % :topic topic) ms)))))

(defn- deserialise
  [{:keys [^Serde key-serde ^Serde value-serde]} topic {:keys [key value]}]
  {:key (.deserialize ^Deserializer (.deserializer key-serde) topic key)
   :value (.deserialize ^Deserializer (.deserializer value-serde) topic value)})

(defn- deserialise-output
  [opts messages]
  (->> messages
       (map (fn [[topic ms]] [topic (mapv #(deserialise opts topic %) ms)]))
       (into {})))

(defrecord SimpleReadableBatchDriver [opts raw-batch-driver]
  ReadableBatchDriver
  (pipe-inputs [_ messages]
    (->> messages
         flatten-input
         (serialise-input opts)
         (rp/pipe-raw-inputs raw-batch-driver)
         (deserialise-output opts)))
  (advance-time [_ advance-millis]
    (deserialise-output opts (rp/advance-time raw-batch-driver advance-millis)))
  (close [_] (rp/close raw-batch-driver)))

(defn processor
  [opts name-topology-supplier-pairs initial-epoch]
  (let [raw-processor (rp/raw-processor (str (gensym)) opts name-topology-supplier-pairs initial-epoch)]
    (->SimpleReadableBatchDriver opts raw-processor)))

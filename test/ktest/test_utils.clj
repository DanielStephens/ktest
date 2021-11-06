(ns ktest.test-utils
  (:require [jackdaw.streams :as j]
            [jackdaw.serdes :as serdes])
  (:import [org.apache.kafka.streams.processor ProcessorContext PunctuationType Punctuator To]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams StreamsBuilder]))

(def edn-serde (serdes/edn-serde))

(def serde-config
  {:key-serde edn-serde
   :value-serde edn-serde})

(defn topic-config
  [topic-name]
  (assoc serde-config
    :topic-name topic-name))

(defn build-topology
  [builder]
  (.build ^StreamsBuilder (j/streams-builder* builder)))

(defn transformer [f]
  (fn [] (let [s (atom {})]
           (reify Transformer
             (init [_ ctx] (swap! s assoc :ctx ctx))
             (transform [_ k v] (f (:ctx @s) k v))
             (close [_])))))

(defn punctuator [delay f]
  (fn [] (reify Transformer
           (init [_ ctx]
             (.schedule ^ProcessorContext ctx
                        ^long delay PunctuationType/WALL_CLOCK_TIME
                        (reify Punctuator
                          (punctuate [_ epoch] (f ctx epoch)))))
           (transform [_ _ _])
           (close [_]))))
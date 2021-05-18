(ns ktest.internal.interop
  (:require [ktest.properties :as props])
  (:import [org.apache.kafka.streams TopologyInternalsAccessor TopologyTestDriver CapturingStreamTask Topology]
           [java.util Properties]
           [org.apache.kafka.streams.processor.internals StreamTask]
           [java.lang.reflect Field Modifier]))

(defn- set-stream-task
  [^TopologyTestDriver ttd ^StreamTask task]
  (let [mf (.getDeclaredField Field "modifiers")
        tf (.getDeclaredField TopologyTestDriver "task")]
    (.setAccessible mf true)
    (.setAccessible tf true)
    (.setInt mf tf (bit-and (.getInt mf tf) (bit-not Modifier/FINAL)))
    (.set tf ttd task)))

(defn test-driver [topology config epoch-millis output-capture]
  (let [t (TopologyTestDriver. ^Topology topology
                               ^Properties (props/properties config)
                               ^long epoch-millis)
        task (TopologyInternalsAccessor/getTestStreamTask t)
        wrapped-task (CapturingStreamTask. task output-capture)]
    (set-stream-task t wrapped-task)
    t))

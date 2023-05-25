(ns ktest.internal.interop
  (:import (java.lang.reflect
            Field
            Modifier)
           (java.util
            Properties)
           (org.apache.kafka.streams
            Topology
            TopologyInternalsAccessor
            TopologyTestDriver)
           (org.apache.kafka.streams.processor.internals
            CapturingStreamTask
            StreamTask)))

(def field-modifiers-field
  (doto (.getDeclaredField Field "modifiers")
    (.setAccessible true)))

(def test-driver-task-field
  (let [tf (.getDeclaredField TopologyTestDriver "task")]
    (.setAccessible tf true)
    (.setInt field-modifiers-field tf
             (bit-and (.getInt field-modifiers-field tf)
                      (bit-not Modifier/FINAL)))
    tf))

(defn get-field
  ([obj c field-name]
   (.get ^Field (doto (.getDeclaredField c field-name)
                  (.setAccessible true))
         obj))
  ([obj field-name]
   (get-field obj (class obj) field-name)))

(defn- set-stream-task
  [^TopologyTestDriver test-driver ^StreamTask task]
  (.set test-driver-task-field test-driver task))

(defn- properties
  [p]
  (reduce-kv
   (fn [p k v]
     (doto p
       (.setProperty (name k) v)))
   (Properties.)
   p))

(defn test-driver
  [topology config epoch-millis output-capture]
  (let [t (TopologyTestDriver. ^Topology topology
                               ^Properties (properties config)
                               ^Long epoch-millis)
        task (TopologyInternalsAccessor/getTestStreamTask t)
        wrapped-task (when task (CapturingStreamTask. task get-field output-capture))]
    (set-stream-task t wrapped-task)
    t))

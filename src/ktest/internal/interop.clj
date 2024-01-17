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

(defn get-private-field
  [t n]
  (doto (.getDeclaredField t n)
    (.setAccessible true)))

(def field-modifiers-field
  (get-private-field Field "modifiers"))

(defn make-field-settable
  [field]
  (.setInt field-modifiers-field field
           (bit-and (.getInt field-modifiers-field field)
                    (bit-not Modifier/FINAL)))
  field)

(def test-driver-task-field
  (-> (get-private-field TopologyTestDriver "task")
      (make-field-settable)))

(defn get-field
  ([obj c field-name]
   (.get ^Field (get-private-field c field-name) obj))
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

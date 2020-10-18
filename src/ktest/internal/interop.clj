(ns ktest.internal.interop
  (:require [ktest.properties :as props])
  (:import [org.apache.kafka.streams TopologyInternalsAccessor TopologyTestDriver CapturingStreamTask]))

(defn test-driver [topology config epoch-millis output-capture]
  (let [t (TopologyTestDriver. topology (props/properties config) epoch-millis)
        task (TopologyInternalsAccessor/getTestStreamTask t)
        wrapped-task (CapturingStreamTask. task output-capture)]
    (TopologyInternalsAccessor/setTestStreamTask t wrapped-task)
    t))

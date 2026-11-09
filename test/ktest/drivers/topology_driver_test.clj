(ns ktest.drivers.topology-driver-test
  (:require [clojure.test :refer :all]
            [ktest.config :refer [mk-opts]]
            [ktest.drivers.topology-driver :as sut]
            [ktest.protocols.driver :as driver]
            [ktest.test-utils :as j])
  (:import (java.nio.charset
            StandardCharsets)
           (java.nio.file
            Files)
           (java.nio.file.attribute
            FileAttribute)
           (org.apache.kafka.common.header.internals
            RecordHeader)
           (org.apache.kafka.streams.processor.api
            FixedKeyRecord)))

(def opts (mk-opts j/serde-config))

(defn very-simple-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "input"))
        (j/to (j/topic-config "output")))
    (j/build-topology builder)))

(deftest driving-simple-topology
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 very-simple-topology
                                 opts)]

    (is (= {"output" [{:key "k"
                       :value "v1"}]}
           (driver/pipe-input driver "input" {:key "k" :value "v1"})))))

(defn repartition-transform-topology
  []
  (let [builder (j/streams-builder)
        kt (j/ktable builder (j/topic-config "table-input"))]
    (-> (j/kstream builder (j/topic-config "stream-input"))
        (j/select-key (constantly "constant"))
        (j/left-join kt
                     (fn [a b]
                       {:stream a
                        :table b})
                     j/serde-config
                     j/serde-config)
        (j/to (j/topic-config "join-output")))
    (j/build-topology builder)))

(deftest driving-topology-with-join
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 repartition-transform-topology
                                 opts)]
    (is (= {}
           (driver/pipe-input driver "table-input" {:key "constant" :value "table1"})))

    (is (= {{:repartition true
             :application-id "application-id"
             :topic-name "KSTREAM-KEY-SELECT-0000000003-repartition"}
            [{:key     "constant"
              :value   "v1"
              :headers {}}]}
           (driver/pipe-input driver "stream-input" {:key "k" :value "v1"})))

    (is (= {"join-output" [{:key "constant"
                            :value {:stream "v1"
                                    :table "table1"}}]}
           (driver/pipe-input driver
                              {:repartition true
                               :application-id "application-id"
                               :topic-name "KSTREAM-KEY-SELECT-0000000003-repartition"}
                              {:key "constant"
                               :value "v1"})))
    (is (= {"table-input" {"partition-id" {"constant" "table1"}}}
           (driver/stores-info driver)))))

(deftest headers-captured-in-repartition
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 repartition-transform-topology
                                 opts)]
    (driver/pipe-input driver "table-input" {:key "constant" :value "table1"})
    (let [result (driver/pipe-input driver "stream-input"
                                    {:key     "k"
                                     :value   "v1"
                                     :headers {"trace-id" "abc-123"}})]
      (is (= {"trace-id" "abc-123"}
             (-> result vals first first :headers))))))

(defn topology-with-store-not-used
  []
  (let [builder (j/streams-builder)
        kt (j/ktable builder (j/topic-config "table-input"))]
    (j/add-store "foo")
    (-> (j/kstream builder (j/topic-config "stream-input"))
        (j/select-key (constantly "constant"))
        (j/left-join kt
                     (fn [a b]
                       {:stream a
                        :table b})
                     j/serde-config
                     j/serde-config)
        (j/to (j/topic-config "join-output")))
    (j/build-topology builder)))

(deftest error-thrown-when-getting-store-info-with-unused-store
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 topology-with-store-not-used
                                 opts)]
    (driver/stores-info driver)))

(defn header-setting-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "input"))
        (j/process-values
         (fn [^FixedKeyRecord record]
           (.add (.headers record) (RecordHeader. "audit.test" (.getBytes "hello" StandardCharsets/UTF_8)))))
        (j/to (j/topic-config "output")))
    (j/build-topology builder)))

(deftest map->headers-round-trip
  (is (= {"audit.event.id" "evt-1"
           "audit.event.type" "party-create"
           "nullable" nil}
          (->> {"audit.event.id" "evt-1"
                "audit.event.type" "party-create"
                "nullable" nil}
               (sut/map->headers)
               (sut/headers->map))))
  (is (nil? (sut/map->headers nil)))
  (is (nil? (sut/map->headers {}))))

(deftest headers-sent-on-input
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 very-simple-topology
                                 opts)]
    (let [result (driver/pipe-input driver "input"
                                    {:key "k"
                                     :value {:foo "bar"}
                                     :headers {"audit.event.id" "evt-1"}})
          output-msg (-> result (get "output") first)]
      (is (= {:foo "bar"} (:value output-msg)))
      (is (= {"audit.event.id" "evt-1"}
             (:kafka-headers (meta output-msg)))))))

(deftest headers-captured-as-metadata
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 header-setting-topology
                                 opts)]
    (let [result (driver/pipe-input driver "input" {:key "k" :value {:foo "bar"}})
          output-msg (-> result (get "output") first)]
      (is (= {:foo "bar"} (:value output-msg)))
      (is (= {"audit.test" "hello"}
             (:kafka-headers (meta output-msg)))))))

(defn global-table-join-topology
  []
  (let [builder (j/streams-builder)
        gkt (j/global-ktable builder (j/topic-config "global-input"))]
    (-> (j/kstream builder (j/topic-config "input"))
        (j/left-join-global gkt
                            (fn [[k _]] k)
                            (fn [v g] {:value v :global g}))
        (j/to (j/topic-config "output")))
    (j/build-topology builder)))

(deftest drivers-with-the-same-application-id-do-not-share-global-store-state
  ;; Neither driver is closed, as is typical of test suites, so on-disk state
  ;; from the first must not be visible to the second.
  (let [new-driver #(sut/driver "same-application-id" "same-partition-id" global-table-join-topology opts)
        first-driver (new-driver)
        _ (driver/pipe-input first-driver "global-input" {:key "k" :value "from-first-driver"})
        _ (is (= {"output" [{:key "k" :value {:value "v" :global "from-first-driver"}}]}
                 (driver/pipe-input first-driver "input" {:key "k" :value "v"})))
        second-driver (new-driver)]
    (is (= {"output" [{:key "k" :value {:value "v" :global nil}}]}
           (driver/pipe-input second-driver "input" {:key "k" :value "v"})))))

(deftest a-state-dir-supplied-by-the-topology-is-kept
  (let [dir (str (Files/createTempDirectory "ktest-supplied-" (make-array FileAttribute 0)))]
    (with-open [driver (sut/driver "application-id" "partition-id"
                                   (fn []
                                     {:topology (global-table-join-topology)
                                      :config {"state.dir" dir}})
                                   opts)]
      (driver/pipe-input driver "global-input" {:key "k" :value "g"})
      (is (.exists (java.io.File. dir "application-id-partition-id"))))))

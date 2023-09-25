(ns ktest.drivers.topology-driver-test
  (:require [clojure.test :refer :all]
            [ktest.config :refer [mk-opts]]
            [ktest.drivers.topology-driver :as sut]
            [ktest.protocols.driver :as driver]
            [ktest.test-utils :as j]))

(def opts (mk-opts j/serde-config))

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

(deftest driver-test
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 repartition-transform-topology
                                 opts)]
    (is (= {}
           (driver/pipe-input driver "table-input" {:key "constant" :value "table1"})))

    (is (= {{:repartition true
             :application-id "application-id"
             :topic-name "KSTREAM-KEY-SELECT-0000000003-repartition"}
            [{:key "constant"
              :value "v1"}]}
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


(ns ktest.drivers.topology-driver-test
  (:require [clojure.test :refer :all]
            [ktest.test-utils :refer :all :as j]
            [ktest.drivers.topology-driver :as sut]
            [ktest.config :refer [mk-opts]]
            [ktest.protocols.driver :refer :all]
            [ktest.serde :refer :all]))

(def opts (mk-opts serde-config))

(defn send-with-serde
  [driver topic msg]
  (deserialise-output opts (pipe-input driver topic (serialise opts topic msg))))

(defn repartition-transform-topology
  []
  (let [builder (j/streams-builder)
        kt (j/ktable builder (topic-config "table-input"))]
    (-> (j/kstream builder (topic-config "stream-input"))
        (j/select-key (constantly "constant"))
        (j/left-join kt
                     (fn [a b] {:stream a
                                :table b})
                     serde-config serde-config)
        (j/to (topic-config "join-output")))
    (build-topology builder)))

(deftest driver-test
  (with-open [driver (sut/driver "application-id"
                                 "partition-id"
                                 repartition-transform-topology
                                 opts)]
    (is (= {}
           (send-with-serde driver "table-input" {:key "constant" :value "table1"})))

    (is (= {{:repartition true
             :application-id "application-id"
             :topic-name "KSTREAM-KEY-SELECT-0000000003-repartition"}
            [{:key "constant"
              :value "v1"}]}
           (send-with-serde driver "stream-input" {:key "k" :value "v1"})))

    (is (= {"join-output" [{:key "constant"
                            :value {:stream "v1"
                                    :table "table1"}}]}
           (send-with-serde driver
                            {:repartition true
                             :application-id "application-id"
                             :topic-name "KSTREAM-KEY-SELECT-0000000003-repartition"}
                            {:key "constant"
                             :value "v1"})))))

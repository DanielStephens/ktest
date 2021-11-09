(ns ktest.batch-drivers.shuffle-driver-test
  (:require [clojure.test :refer :all]
            [ktest.protocols.batch-driver :refer :all]
            [ktest.batch-drivers.shuffle-driver :as sut]))

(def noop-delegate
  (reify
    BatchDriver
    (pipe-inputs [_ messages]
      messages)
    (advance-time [_ _])
    (current-time [_] 0)
    (close [_])))

(defn shuffle-driver
  [seed]
  (sut/batch-driver noop-delegate
                    {:partition (fn default-partition-strategy
                                  [_ {:keys [key]}]
                                  key)
                     :seed seed}))

(deftest no-messages-is-safe
  (let [driver (shuffle-driver 4096)]
    (is (= []
           (pipe-inputs driver [])))))

(deftest shuffling-test
  (testing "driver shuffles topics"
    (let [driver (shuffle-driver 4096)]
      (is (= [{:key 1 :value 1 :topic "a"}
              {:key 0 :value 0 :topic "a"}]
             (pipe-inputs driver
                          [{:key 0 :value 0 :topic "a"}
                           {:key 1 :value 1 :topic "a"}])))))

  (testing "driver does not shuffle partitions"
    (let [driver (shuffle-driver 4096)]
      (is (= [{:key 0 :value 0 :topic "a"}
              {:key 0 :value 1 :topic "a"}]
             (pipe-inputs driver
                          [{:key 0 :value 0 :topic "a"}
                           {:key 0 :value 1 :topic "a"}])))))

  (testing "driver shuffles topics together"
    (let [driver (shuffle-driver 1)]
      (is (= [{:topic "a" :key 1 :value 2}
              {:topic "b" :key 0 :value 0}
              {:topic "a" :key 0 :value 0}
              {:topic "b" :key 1 :value 1}
              {:topic "a" :key 0 :value 1}]
             (pipe-inputs driver
                          [{:key 0 :value 0 :topic "a"}
                           {:key 0 :value 1 :topic "a"}
                           {:key 1 :value 2 :topic "a"}
                           {:key 0 :value 0 :topic "b"}
                           {:key 1 :value 1 :topic "b"}]))))))

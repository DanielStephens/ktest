(ns ktest.core-test
  (:require [clojure.test :refer :all]
            [ktest.core :as sut]
            [ktest.test-utils :as j])
  (:import (java.time
            Duration)
           (org.apache.kafka.streams.processor
            ProcessorContext)
           (org.apache.kafka.streams.state
            KeyValueStore
            Stores
            ValueAndTimestamp)))

(defn unused-topology
  []
  (let [builder (j/streams-builder)]
    (j/kstream builder (j/topic-config "unused-input"))
    (j/build-topology builder)))

(deftest unused
  (with-open [driver (sut/driver j/serde-config
                                 {"unused" unused-topology})]
    (is (= {} (sut/pipe driver "unused-input" {:key "k" :value "v"})))))

(defn simple-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "simple-input"))
        (j/map (fn [[k v]] [(str k " = key") (str v " = value")]))
        (j/to (j/topic-config "simple-output")))
    (j/build-topology builder)))

(deftest simple
  (with-open [driver (sut/driver j/serde-config
                                 {"simple" simple-topology})]
    (is (= {"simple-output" [{:key "k = key"
                              :value "v = value"}]}
           (sut/pipe driver "simple-input" {:key "k" :value "v"})))))

(defn through-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "through-input"))
        (j/through (j/topic-config "through"))
        (j/to (j/topic-config "through-output")))
    (j/build-topology builder)))

(deftest through
  (with-open [driver (sut/driver j/serde-config
                                 {"through" through-topology})]
    (is (= {"through" [{:key "k"
                        :value "v"}]
            "through-output" [{:key "k"
                               :value "v"}]}
           (sut/pipe driver "through-input" {:key "k" :value "v"})))))

(defn agg-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "agg-input"))
        (j/group-by-key)
        (j/aggregate (constantly [])
                     (fn [agg [k v]] (conj agg {:k k :v v}))
                     (j/topic-config "agg"))
        (j/to-kstream)
        (j/to (j/topic-config "agg-output")))
    (j/build-topology builder)))

(defn select-key-agg-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "agg-input"))
        (j/select-key (constantly 0))
        (j/group-by-key j/serde-config)
        (j/aggregate (constantly [])
                     (fn [agg [k v]] (conj agg {:k k :v v}))
                     (j/topic-config "agg"))
        (j/to-kstream)
        (j/to (j/topic-config "agg-output")))
    (j/build-topology builder)))

(deftest aggregation
  (testing "just aggregate"
    (with-open [driver (sut/driver j/serde-config
                                   {"agg" agg-topology})]
      (is (= {"agg-output" [{:key "k"
                             :value [{:k "k"
                                      :v "v1"}]}]}
             (sut/pipe driver "agg-input" {:key "k" :value "v1"})))
      (is (= {"agg-output" [{:key "k"
                             :value [{:k "k"
                                      :v "v1"}
                                     {:k "k"
                                      :v "v2"}]}]}
             (sut/pipe driver "agg-input" {:key "k" :value "v2"})))
      (is (= {"agg-output" [{:key "other"
                             :value [{:k "other"
                                      :v "v3"}]}]}
             (sut/pipe driver "agg-input" {:key "other" :value "v3"})))))

  (testing "select-key then aggregate"
    (with-open [driver (sut/driver j/serde-config
                                   {"select-key-agg" select-key-agg-topology})]
      (is (= {"agg-output" [{:key 0
                             :value [{:k 0
                                      :v "v1"}]}]}
             (sut/pipe driver "agg-input" {:key "k" :value "v1"})))
      (is (= {"agg-output" [{:key 0
                             :value [{:k 0
                                      :v "v1"}
                                     {:k 0
                                      :v "v2"}]}]}
             (sut/pipe driver "agg-input" {:key "other" :value "v2"}))))))

(defn transform-topology
  []
  (let [builder (j/streams-builder)]
    (.addStateStore builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "trans-store")
                     j/edn-serde j/edn-serde))
    (-> (j/kstream builder (j/topic-config "trans-input"))
        (j/transform
         (j/transformer
          (fn [^ProcessorContext ctx k v]
            (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                  current (or (.get store "constant") [])
                  next (conj current {:k k :v v})]
              (.put store "constant" next)
              (.forward ctx k next))))
         ["trans-store"])
        (j/to (j/topic-config "trans-output")))
    (j/build-topology builder)))

(defn select-key-transform-topology
  []
  (let [builder (j/streams-builder)]
    (.addStateStore builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "trans-store")
                     j/edn-serde j/edn-serde))
    (-> (j/kstream builder (j/topic-config "trans-input"))
        (j/select-key (constantly "constant"))
        (j/transform
         (j/transformer
          (fn [^ProcessorContext ctx k v]
            (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                  current (or (.get store "constant") [])
                  next (conj current {:k k :v v})]
              (.put store "constant" next)
              (.forward ctx k next))))
         ["trans-store"])
        (j/to (j/topic-config "trans-output")))
    (j/build-topology builder)))

(defn repartition-transform-topology
  []
  (let [builder (j/streams-builder)]
    (.addStateStore builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "trans-store")
                     j/edn-serde j/edn-serde))
    (-> (j/kstream builder (j/topic-config "trans-input"))
        (j/select-key (constantly "constant"))
        (j/through (j/topic-config "through"))
        (j/transform
         (j/transformer
          (fn [^ProcessorContext ctx k v]
            (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                  current (or (.get store "constant") [])
                  next (conj current {:k k :v v})]
              (.put store "constant" next)
              (.forward ctx k next))))
         ["trans-store"])
        (j/to (j/topic-config "trans-output")))
    (j/build-topology builder)))

(deftest transform
  (testing "transform"
    (with-open [driver (sut/driver j/serde-config
                                   {"transform" transform-topology})]
      (is (= {"trans-output" [{:key "k"
                               :value [{:k "k"
                                        :v "v1"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v1"})))
      (is (= {"trans-output" [{:key "k"
                               :value [{:k "k"
                                        :v "v1"}
                                       {:k "k"
                                        :v "v2"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v2"})))
      (is (= {"trans-output" [{:key "other"
                               :value [{:k "other"
                                        :v "v3"}]}]}
             (sut/pipe driver "trans-input" {:key "other" :value "v3"})))))

  (testing "transform with select-key"
    (with-open [driver (sut/driver j/serde-config
                                   {"transform-select-key" select-key-transform-topology})]
      (is (= {"trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v1"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v1"})))
      (is (= {"trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v1"}
                                       {:k "constant"
                                        :v "v2"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v2"})))
      (is (= {"trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v3"}]}]}
             (sut/pipe driver "trans-input" {:key "other" :value "v3"})))))

  (testing "transform with repartition"
    (with-open [driver (sut/driver j/serde-config
                                   {"transform-repartition" repartition-transform-topology})]
      (is (= {"through" [{:key "constant"
                          :value "v1"}]
              "trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v1"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v1"})))
      (is (= {"through" [{:key "constant"
                          :value "v2"}]
              "trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v1"}
                                       {:k "constant"
                                        :v "v2"}]}]}
             (sut/pipe driver "trans-input" {:key "k" :value "v2"})))
      (is (= {"through" [{:key "constant"
                          :value "v3"}]
              "trans-output" [{:key "constant"
                               :value [{:k "constant"
                                        :v "v1"}
                                       {:k "constant"
                                        :v "v2"}
                                       {:k "constant"
                                        :v "v3"}]}]}
             (sut/pipe driver "trans-input" {:key "other" :value "v3"}))))))

(defn first-connected-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "connected-input"))
        (j/map (fn [[k v]] [(str k " in first") (str v " in first")]))
        (j/to (j/topic-config "connection")))
    (j/build-topology builder)))

(defn second-connected-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "connection"))
        (j/map (fn [[k v]] [(str k " then in second") (str v " then in second")]))
        (j/to (j/topic-config "connected-output")))
    (j/build-topology builder)))

(deftest connecting
  (with-open [driver (sut/driver j/serde-config
                                 {"first" first-connected-topology
                                  "second" second-connected-topology})]
    (is (= {"connection" [{:key "k in first"
                           :value "v in first"}]
            "connected-output" [{:key "k in first then in second"
                                 :value "v in first then in second"}]}
           (sut/pipe driver "connected-input" {:key "k" :value "v"})))))

(defn advance-time-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "time-input"))
        (j/transform
         (j/punctuator (Duration/ofMillis 1) (fn [ctx epoch]
                                               (.forward ctx "k" (str "v at " epoch)))))
        (j/to (j/topic-config "time-output")))
    (j/build-topology builder)))

(deftest advance-time
  (with-open [driver (sut/driver j/serde-config
                                 {"advance-time" advance-time-topology})]
    (is (= {"time-output" [{:key "k"
                            :value "v at 1"}]}
           (sut/advance-time driver 1)))))

(defn empty-topo
  []
  (let [builder (j/streams-builder)]
    (j/build-topology builder)))

(deftest empty-topology-with-no-stream-task
  (with-open [driver (sut/driver j/serde-config
                                 {"empty-topo" empty-topo})]
    (is (= {}
           (sut/advance-time driver 1)))))

(defn join-topology
  []
  (let [builder (j/streams-builder)
        table (j/ktable builder (j/topic-config "table-input"))]
    (-> (j/kstream builder (j/topic-config "join-input"))
        (j/left-join table (fn [i t]
                             {:input i
                              :table-value t}))
        (j/to (j/topic-config "join-output")))
    (j/build-topology builder)))

(defn select-key-join-topology
  []
  (let [builder (j/streams-builder)
        _unused-table (-> (j/kstream builder (j/topic-config "table-input"))
                          (j/select-key (fn [[_ _]] (rand-int 1000)))
                          (j/map-values :input)
                          (j/group-by-key j/serde-config)
                          (j/reduce (fn [_ b] b) (j/topic-config "a")))
        table (-> (j/kstream builder (j/topic-config "table-input"))
                  (j/select-key (fn [[_ v]] (:input v)))
                  (j/map-values :input)
                  (j/group-by-key j/serde-config)
                  (j/reduce (fn [_ b] b) (j/topic-config "table")))]
    (-> (j/kstream builder (j/topic-config "join-input"))
        (j/select-key (fn [[_ v]] v))
        (j/left-join table (fn [v t]
                             {:input v
                              :table-value t})
                     j/serde-config j/serde-config)
        (j/through (j/topic-config "join-output"))
        (j/to (j/topic-config "table-input")))
    (j/build-topology builder)))

(deftest join
  (testing "join"
    (with-open [driver (sut/driver j/serde-config
                                   {"join" join-topology})]
      (is (= {} (sut/pipe driver "table-input" {:key ["k" "p"] :value "v in table"})))
      (is (= {"join-output" [{:key ["k" "p"]
                              :value {:input "v in input"
                                      :table-value "v in table"}}]}
             (sut/pipe driver "join-input" {:key ["k" "p"] :value "v in input"})))
      (is (= {"join-output" [{:key "k2"
                              :value {:input "v in input"
                                      :table-value nil}}]}
             (sut/pipe driver "join-input" {:key "k2" :value "v in input"})))))

  (testing "join-with-select-key"
    (with-open [driver (sut/driver j/serde-config
                                   {"join" select-key-join-topology})]
      (is (= {"join-output" [{:key "id"
                              :value {:input "id"
                                      :table-value nil}}]
              "table-input" [{:key "id"
                              :value {:input "id"
                                      :table-value nil}}]}
             (sut/pipe driver "join-input" {:key "k" :value "id"})))
      (is (= {"join-output" [{:key "id"
                              :value {:input "id"
                                      :table-value "id"}}]
              "table-input" [{:key "id"
                              :value {:input "id"
                                      :table-value "id"}}]}
             (sut/pipe driver "join-input" {:key "k" :value "id"}))))))

(defn global-kt-topology
  []
  (let [builder (j/streams-builder)
        kt (j/ktable builder (j/topic-config "normal-input"))
        gkt (j/global-ktable builder (j/topic-config "global-input"))
        gkt2 (j/global-ktable builder (j/topic-config "global-input-2"))]
    (-> (j/kstream builder (j/topic-config "join-input"))
        (j/map (fn [[k v]] [k {:stream v}]))
        (j/left-join kt (fn [m b] (assoc m :normal-ktable b))
                     j/serde-config j/serde-config)
        (j/left-join-global gkt
                            (comp :stream second)
                            (fn [m b] (assoc m :global-ktable b)))
        (j/left-join-global gkt2
                            (comp :stream second)
                            (fn [m b] (assoc m :global-ktable-2 b)))
        (j/to (j/topic-config "join-output")))
    (j/build-topology builder)))

(deftest global-kt
  (with-open [driver (sut/driver j/serde-config
                                 {"global-kt" global-kt-topology})]
    (is (= {"join-output" [{:key "k"
                            :value {:stream "global-key"
                                    :normal-ktable nil
                                    :global-ktable nil
                                    :global-ktable-2 nil}}]}
           (sut/pipe driver "join-input" {:key "k" :value "global-key"})))

    (sut/pipe driver "normal-input" {:key "k" :value "normal-value"})
    (sut/pipe driver "normal-input" {:key "not-k" :value "other-normal-value"})
    (sut/pipe driver "global-input" {:key "global-key" :value "global-value"})
    (sut/pipe driver "global-input-2" {:key "global-key" :value "global-value-2"})

    (is (= {"join-output" [{:key "k"
                            :value {:stream "global-key"
                                    :normal-ktable "normal-value"
                                    :global-ktable "global-value"
                                    :global-ktable-2 "global-value-2"}}]}
           (sut/pipe driver "join-input" {:key "k" :value "global-key"})))))

(defn get-from-store
  [ctx store-name k]
  (let [^KeyValueStore store (.getStateStore ctx store-name)
        ^ValueAndTimestamp vt (.get store k)]
    (when vt (.value vt))))

(defn recursive-advance-time-topology
  []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (j/topic-config "trigger-1-input"))
        (j/group-by-key)
        (j/aggregate (constantly nil)
                     (fn [_ [_ v]] v)
                     (j/topic-config "trigger-1-store")))

    (-> (j/kstream builder (j/topic-config "empty"))
        (j/transform
         (j/punctuator (Duration/ofMillis 1)
                       (fn [ctx timestamp]
                         (when-let [t1-value (get-from-store ctx "trigger-1-store" "key")]
                           (.forward ctx
                                     "key"
                                     {:t1-timestamp timestamp
                                      :t1-value t1-value}))))
         ["trigger-1-store"])
        (j/to (j/topic-config "trigger-2-input")))

    (-> (j/kstream builder (j/topic-config "trigger-2-input"))
        (j/group-by-key)
        (j/aggregate (constantly nil)
                     (fn [_ [_ v]] v)
                     (j/topic-config "trigger-2-store")))

    (-> (j/kstream builder (j/topic-config "empty"))
        (j/transform
         (j/punctuator (Duration/ofMillis 1)
                       (fn [ctx timestamp]
                         (when-let [t1-map (get-from-store ctx "trigger-2-store" "key")]
                           (.forward ctx
                                     "key"
                                     (assoc t1-map :t2-timestamp timestamp)))))
         ["trigger-2-store"])
        (j/to (j/topic-config "output")))

    (j/build-topology builder)))

(deftest recursive-advance-time
  (with-open [driver (sut/driver j/serde-config
                                 {"advance-time" recursive-advance-time-topology})]
    (is (= {}
           (sut/advance-time driver 1)))

    (sut/pipe driver "trigger-1-input" {:key "key" :value "value"})
    (is (= {"trigger-2-input" [{:key "key"
                                :value {:t1-timestamp 2
                                        :t1-value "value"}}]}
           (sut/advance-time driver 1)))

    (is (= {"trigger-2-input" [{:key "key"
                                :value {:t1-timestamp 3
                                        :t1-value "value"}}]
            "output" [{:key "key"
                       :value {:t1-timestamp 2
                               :t1-value "value"
                               :t2-timestamp 3}}]}
           (sut/advance-time driver 1)))))

(defn- range-key
  [foreign-key position & more]
  ;; Uses a swap between zero '0', one '1', and two '2' to be able to produce
  ;; keys that are guaranteed to be before/within/after the foreign-key based
  ;; range when querying with RocksDB.
  (let [delimiter \b
        p-char (case position
                 :before \0
                 :within \1
                 :after \2)]
    (str foreign-key delimiter
         p-char delimiter
         more)))

(defn within-range-key
  [foreign-key primary-key]
  (range-key foreign-key :within primary-key))

(defn before-range-key
  [foreign-key]
  (range-key foreign-key :before))

(defn after-range-key
  [foreign-key]
  (range-key foreign-key :after))

(defn store-by-range-key
  [^ProcessorContext ctx store-name foreign-key original-key original-value]
  (let [store (.getStateStore ctx store-name)
        k (within-range-key foreign-key original-key)]
    (.put store k original-value)))

(defn range-topology
  []
  (let [builder (j/streams-builder)]
    (.addStateStore builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "trans-store")
                     j/edn-serde j/edn-serde))
    (-> (j/kstream builder (j/topic-config "range-input"))
        (j/transform
         (j/transformer
          (fn [^ProcessorContext ctx k v]
            (store-by-range-key ctx "trans-store" k (:original-key v) v)
            nil))
         ["trans-store"]))
    (-> (j/kstream builder (j/topic-config "input"))
        (j/transform
         (j/transformer
          (fn [^ProcessorContext ctx k v]
            (let [^KeyValueStore store (.getStateStore ctx "trans-store")]
              (doseq [kv-java (iterator-seq (.range store (before-range-key k) (after-range-key k)))]
                (.forward ctx (.key kv-java) (.value kv-java))))))
         ["trans-store"])
        (j/to (j/topic-config "output")))
    (j/build-topology builder)))

(deftest range-test
  (testing "simple-single-range-string"
    (let [partition-key "determines-partition"]
      (with-open [driver (sut/driver j/serde-config
                                     {"transform" range-topology})]
        (is (= {}
               (sut/pipe driver "range-input" {:key partition-key
                                               :value {:original-key "unique-key-1"
                                                       :foo :bar}})))
        (is (= {"output" [{:key (within-range-key partition-key "unique-key-1")
                           :value {:original-key "unique-key-1"
                                   :foo :bar}}]}
               (sut/pipe driver "input" {:key partition-key :value "v2"}))))))
  (testing "simple-single-range-vec"
    (let [partition-key ["foo" "bar"]]
      (with-open [driver (sut/driver j/serde-config
                                     {"transform" range-topology})]
        (is (= {}
               (sut/pipe driver "range-input" {:key partition-key
                                               :value {:original-key "unique-key-1"
                                                       :foo :bar}})))
        (is (= {"output" [{:key (within-range-key partition-key "unique-key-1")
                           :value {:original-key "unique-key-1"
                                   :foo :bar}}]}
               (sut/pipe driver "input" {:key partition-key :value "v2"}))))))
  (testing "simple-multiple-range-int"
    (let [partition-key "determines-partition"]
      (with-open [driver (sut/driver j/serde-config
                                     {"transform" range-topology})]
        (doseq [i (range 10)]
          (is (= {}
                 (sut/pipe driver "range-input" {:key partition-key
                                                 :value {:original-key i
                                                         :foo :bar}}))))
        (is (= {"output" (map
                          (fn [i]
                            {:key (within-range-key partition-key i)
                             :value {:original-key i
                                     :foo :bar}})
                          (range 10))}
               (sut/pipe driver "input" {:key partition-key :value "v2"})))))))

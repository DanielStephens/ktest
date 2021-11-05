(ns ktest.core-test
  (:require [clojure.test :refer :all]
            [ktest.core :as sut]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as serdes])
  (:import [org.apache.kafka.streams StreamsBuilder]
           [org.apache.kafka.streams.state Stores KeyValueStore]
           [org.apache.kafka.streams.kstream TransformerSupplier Transformer]
           [org.apache.kafka.streams.processor ProcessorContext PunctuationType Punctuator]))

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

(defn unused-topology []
  (let [builder (j/streams-builder)]
    (j/kstream builder (topic-config "unused-input"))
    (build-topology builder)))

(deftest unused
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "unused"
                                 unused-topology)]
    (is (= {} (sut/pipe driver "unused-input" {:key "k" :value "v"})))))

(defn simple-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "simple-input"))
        (j/map (fn [[k v]] [(str k " = key") (str v " = value")]))
        (j/to (topic-config "simple-output")))
    (build-topology builder)))

(deftest simple
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "simple"
                                 simple-topology)]
    (is (= {"simple-output" [{:key "k = key"
                              :value "v = value"}]}
           (sut/pipe driver "simple-input" {:key "k" :value "v"})))))

(defn through-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "through-input"))
        (j/through (topic-config "through"))
        (j/to (topic-config "through-output")))
    (build-topology builder)))

(deftest through
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "through"
                                 through-topology)]
    (is (= {"through" [{:key "k"
                        :value "v"}]
            "through-output" [{:key "k"
                               :value "v"}]}
           (sut/pipe driver "through-input" {:key "k" :value "v"})))))

(defn agg-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "agg-input"))
        (j/group-by-key)
        (j/aggregate (constantly [])
                     (fn [agg [k v]] (conj agg {:k k :v v}))
                     (topic-config "agg"))
        (j/to-kstream)
        (j/to (topic-config "agg-output")))
    (build-topology builder)))

(defn select-key-agg-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "agg-input"))
        (j/select-key (constantly 0))
        (j/group-by-key {:key-serde edn-serde
                         :value-serde edn-serde})
        (j/aggregate (constantly [])
                     (fn [agg [k v]] (conj agg {:k k :v v}))
                     (topic-config "agg"))
        (j/to-kstream)
        (j/to (topic-config "agg-output")))
    (build-topology builder)))

(deftest aggregation
  (testing "just aggregate"
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "agg"
                                   agg-topology)]
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
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "select-key-agg"
                                   select-key-agg-topology)]
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

(defn transform-topology []
  (let [builder (j/streams-builder)]
    (.addStateStore (j/streams-builder* builder)
                    (Stores/keyValueStoreBuilder
                      (Stores/persistentKeyValueStore "trans-store")
                      edn-serde edn-serde))
    (-> (j/kstream builder (topic-config "trans-input"))
        (j/transform
          (transformer
            (fn [^ProcessorContext ctx k v]
              (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                    current (or (.get store "constant") [])
                    next (conj current {:k k :v v})]
                (.put store "constant" next)
                (.forward ctx k next))))
          ["trans-store"])
        (j/to (topic-config "trans-output")))
    (build-topology builder)))

(defn select-key-transform-topology []
  (let [builder (j/streams-builder)]
    (.addStateStore (j/streams-builder* builder)
                    (Stores/keyValueStoreBuilder
                      (Stores/persistentKeyValueStore "trans-store")
                      edn-serde edn-serde))
    (-> (j/kstream builder (topic-config "trans-input"))
        (j/select-key (constantly "constant"))
        (j/transform
          (transformer
            (fn [^ProcessorContext ctx k v]
              (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                    current (or (.get store "constant") [])
                    next (conj current {:k k :v v})]
                (.put store "constant" next)
                (.forward ctx k next))))
          ["trans-store"])
        (j/to (topic-config "trans-output")))
    (build-topology builder)))

(defn repartition-transform-topology []
  (let [builder (j/streams-builder)]
    (.addStateStore (j/streams-builder* builder)
                    (Stores/keyValueStoreBuilder
                      (Stores/persistentKeyValueStore "trans-store")
                      edn-serde edn-serde))
    (-> (j/kstream builder (topic-config "trans-input"))
        (j/select-key (constantly "constant"))
        (j/through (topic-config "through"))
        (j/transform
          (transformer
            (fn [^ProcessorContext ctx k v]
              (let [^KeyValueStore store (.getStateStore ctx "trans-store")
                    current (or (.get store "constant") [])
                    next (conj current {:k k :v v})]
                (.put store "constant" next)
                (.forward ctx k next))))
          ["trans-store"])
        (j/to (topic-config "trans-output")))
    (build-topology builder)))

(deftest transform
  (testing "transform"
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "transform"
                                   transform-topology)]
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
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "transform-select-key"
                                   select-key-transform-topology)]
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
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "transform-repartition"
                                   repartition-transform-topology)]
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

(defn first-connected-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "connected-input"))
        (j/map (fn [[k v]] [(str k " in first") (str v " in first")]))
        (j/to (topic-config "connection")))
    (build-topology builder)))

(defn second-connected-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "connection"))
        (j/map (fn [[k v]] [(str k " then in second") (str v " then in second")]))
        (j/to (topic-config "connected-output")))
    (build-topology builder)))

(deftest connecting
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "first" first-connected-topology
                                 "second" second-connected-topology)]
    (is (= {"connection" [{:key "k in first"
                           :value "v in first"}]
            "connected-output" [{:key "k in first then in second"
                                 :value "v in first then in second"}]}
           (sut/pipe driver "connected-input" {:key "k" :value "v"})))))

(defn advance-time-topology []
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (topic-config "time-input"))
        (j/transform
          (punctuator 1 (fn [ctx epoch]
                          (.forward ctx "k" (str "v at " epoch)))))
        (j/to (topic-config "time-output")))
    (build-topology builder)))

(deftest advance-time
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "advance-time" advance-time-topology)]
    (is (= {"time-output" [{:key "k"
                            :value "v at 1"}]}
           (sut/advance-time driver 1)))))

(defn join-topology []
  (let [builder (j/streams-builder)
        table (j/ktable builder (topic-config "table-input"))]
    (-> (j/kstream builder (topic-config "join-input"))
        (j/left-join table (fn [i t] {:input i
                                      :table-value t}))
        (j/to (topic-config "join-output")))
    (build-topology builder)))

(defn select-key-join-topology []
  (let [builder (j/streams-builder)
        unused-table (-> (j/kstream builder (topic-config "table-input"))
                         (j/select-key (fn [[_ v]] (rand-int 1000)))
                         (j/map-values :input)
                         (j/group-by-key serde-config)
                         (j/reduce (fn [_ b] b) (topic-config "a")))
        table (-> (j/kstream builder (topic-config "table-input"))
                  (j/select-key (fn [[_ v]] (:input v)))
                  (j/map-values :input)
                  (j/group-by-key serde-config)
                  (j/reduce (fn [_ b] b) (topic-config "table")))]
    (-> (j/kstream builder (topic-config "join-input"))
        (j/select-key (fn [[_ v]] v))
        (j/left-join table (fn [v t] {:input v
                                      :table-value t})
                     serde-config serde-config)
        (j/through (topic-config "join-output"))
        (j/to (topic-config "table-input")))
    (build-topology builder)))

(deftest join
  (testing "join"
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "join" join-topology)]
      (is (= {} (sut/pipe driver "table-input" {:key "k" :value "v in table"})))
      (is (= {"join-output" [{:key "k"
                              :value {:input "v in input"
                                      :table-value "v in table"}}]}
             (sut/pipe driver "join-input" {:key "k" :value "v in input"})))
      (is (= {"join-output" [{:key "k2"
                              :value {:input "v in input"
                                      :table-value nil}}]}
             (sut/pipe driver "join-input" {:key "k2" :value "v in input"})))))

  (testing "join-with-select-key"
    (with-open [driver (sut/driver {:key-serde edn-serde
                                    :value-serde edn-serde}
                                   "join" select-key-join-topology)]
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

(defn global-kt-topology []
  (let [builder (j/streams-builder)
        gkt (j/global-ktable builder (topic-config "global-input"))]
    (-> (j/kstream builder (topic-config "join-input"))
        (j/left-join-global gkt
                            (fn [[_ v]] v)
                            (fn [a b] {:stream a
                                       :global-ktable b}))
        (j/to (topic-config "join-output")))
    (build-topology builder)))

(deftest global-kt
  (with-open [driver (sut/driver {:key-serde edn-serde
                                  :value-serde edn-serde}
                                 "global-kt"
                                 global-kt-topology)]
    (is (= {"join-output" [{:key "k"
                           :value {:stream "global-key"
                                   :global-ktable nil}}]}
           (sut/pipe driver "join-input" {:key "k" :value "global-key"})))

    (sut/pipe driver "global-input" {:key "global-key" :value "global-value"})

    (is (= {"join-output" [{:key "k"
                            :value {:stream "global-key"
                                    :global-ktable "global-value"}}]}
           (sut/pipe driver "join-input" {:key "k" :value "global-key"})))))

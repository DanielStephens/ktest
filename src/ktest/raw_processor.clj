(ns ktest.raw-processor
  (:require [ktest.driver :as p]
            [ktest.multi-topology :as mt])
  (:import [java.util Random Collections ArrayList Collection]))

(defn- munge-outputs
  [topic-output-maps]
  (apply merge-with concat topic-output-maps))

(defprotocol BatchDriver
  (pipe-raw-inputs [this messages])
  (advance-time [this advance-millis])
  (close [this]))

(defrecord BatchToSimpleDriver [delegate-driver]
  BatchDriver
  (pipe-raw-inputs [_ messages]
    (->> messages
         (map (fn [m] (p/pipe-raw-input delegate-driver (:topic m) m)))
         (munge-outputs)))
  (advance-time [_ advance-millis]
    (p/advance-time delegate-driver advance-millis))
  (close [_] (p/close delegate-driver)))

(defn- with-batching [driver] (->BatchToSimpleDriver driver))

;; deterministic shuffle

(defn- shuffle-with-random
  [^Collection coll ^Random random]
  (let [al (ArrayList. coll)]
    (Collections/shuffle al random)
    (vec al)))

(defrecord ShuffleDriver [delegate-batch-driver opts random]
  BatchDriver
  (pipe-raw-inputs [_ messages]
    (pipe-raw-inputs delegate-batch-driver
                     (if (:randomise opts)
                       (shuffle-with-random messages random)
                       messages)))
  (advance-time [_ advance-millis]
    (advance-time delegate-batch-driver advance-millis))
  (close [_] (close delegate-batch-driver)))

(defn- random
  [{:keys [seed]}]
  (let [r (Random.)]
    (.setSeed r seed)
    r))

(defn- with-opt-randomness
  [batch-driver opts]
  {:pre [(:seed opts)]}
  (let [r (random opts)]
    (->ShuffleDriver batch-driver opts r)))

;; recursion

(defn- recursively-pipe
  [depth {:keys [recursion-limit recurse] :as opts}
   delegate-batch-driver inputs output]
  (if (> depth recursion-limit)
    (throw (ex-info "still more recursion necessary but safety limit has been reached." {}))
    (let [more-output (pipe-raw-inputs delegate-batch-driver inputs)
          next-input (->> more-output
                          (mapcat (fn [[topic ms]] (map #(assoc % :topic topic) ms))))
          combined-output (munge-outputs [output more-output])]
      (if (and recurse (seq next-input))
        (recur (inc depth) opts delegate-batch-driver
               next-input combined-output)
        combined-output))))

(defrecord RecursionDriver [delegate-batch-driver opts]
  BatchDriver
  (pipe-raw-inputs [_ messages]
    (recursively-pipe 0 opts delegate-batch-driver messages {}))
  (advance-time [_ advance-millis]
    (advance-time delegate-batch-driver advance-millis))
  (close [_] (close delegate-batch-driver)))

(defn- with-opt-recursion
  [batch-driver opts]
  (->RecursionDriver batch-driver opts))

(defn raw-processor
  [id opts name-topology-supplier-pairs initial-epoch]
  (let [d (mt/multi-topology-driver id opts name-topology-supplier-pairs initial-epoch)]
    (-> d
        (with-batching)
        (with-opt-randomness opts)
        (with-opt-recursion opts))))

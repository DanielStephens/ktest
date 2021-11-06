(ns ktest.utils
  (:import [java.util Random]))

(defn genid
  []
  (str (gensym)))

(defn munge-outputs
  [topic-output-maps]
  (apply merge-with concat topic-output-maps))

(defn random
  [seed]
  (let [r (Random.)]
    (.setSeed r seed)
    r))
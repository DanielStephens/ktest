(ns build
  (:require [badigeon.clean :as clean]
            [badigeon.javac :as javac]))

(defn -main []
  (clean/clean "target")
  (javac/javac "src-java"))
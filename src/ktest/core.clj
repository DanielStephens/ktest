(ns ktest.core
  (:require [ktest.driver :refer [default-driver]]
            [ktest.protocols.batch-driver :as b]
            [ktest.config :refer [mk-opts]]))

(defn driver
  [opts & name-topology-supplier-pairs]
  (when-not (even? (count name-topology-supplier-pairs))
    (throw (ex-info "Pairs of topologies and their config maps are expected" {})))
  (default-driver (mk-opts opts)
                  (partition 2 name-topology-supplier-pairs)))

(defn pipe [driver topic message]
  (b/pipe-inputs driver [(assoc message :topic topic)]))

(defn pipe-many [driver messages]
  (b/pipe-inputs driver messages))

(defn advance-time [driver advance-millis]
  (b/advance-time driver advance-millis))

(defn set-time [driver epoch-millis]
  (advance-time driver (- epoch-millis (b/current-time driver))))

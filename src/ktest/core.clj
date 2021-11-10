(ns ktest.core
  (:require [ktest.driver :refer [default-driver]]
            [ktest.protocols.batch-driver :as b]
            [ktest.config :refer [mk-opts]]))

(defn driver
  [opts name-topology-supplier-map]
  {:pre [(or (nil? opts) (map? opts))
         (map? name-topology-supplier-map)
         (every? (comp string? key) name-topology-supplier-map)
         (every? (comp fn? val) name-topology-supplier-map)]}
  (default-driver (mk-opts opts)
                  name-topology-supplier-map))

(defn pipe [driver topic message]
  (b/pipe-inputs driver [(assoc message :topic topic)]))

(defn pipe-many [driver messages]
  (b/pipe-inputs driver messages))

(defn advance-time [driver advance-millis]
  (b/advance-time driver advance-millis))

(defn set-time [driver epoch-millis]
  (advance-time driver (- epoch-millis (b/current-time driver))))

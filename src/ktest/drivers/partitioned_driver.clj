(ns ktest.drivers.partitioned-driver
  (:require [clojure.string :as str]
            [digest :refer [md5]]
            [ktest.drivers.topology-driver :as t]
            [ktest.protocols.driver :refer :all]
            [ktest.utils :refer :all]))

(defn- message-partition
  [opts topic message]
  ((:partition opts) topic message opts))

(defn- partitioned-id
  [partition]
  (md5 (str partition)))

(defn- driver!
  [state partition supplier]
  (let [k [:drivers partition]
        ;; using a delay avoids us doing lots of work repeatedly making the
        ;; test driver if our atom gets conflicts
        new-topology (delay (supplier partition))
        s (swap! state (fn [s] (if (get-in s k) s (assoc-in s k @new-topology))))]
    (get-in s k)))

(defn- drivers
  [state]
  (->> (:drivers @state)
       vals
       seq))

(defrecord PartitioningDriver
  [opts state root-application-id supplier]

  Driver

  (pipe-input
    [_ topic message]
    (let [ptition (message-partition opts topic message)
          driver (driver! state ptition supplier)]
      (pipe-input driver topic message)))


  (advance-time
    [_ advance-millis]
    (swap! state update :epoch + advance-millis)
    (->> (drivers state)
         (map #(advance-time % advance-millis))
         doall
         (munge-outputs)))


  (current-time [_] (:epoch @state))


  (stores-info
    [_]
    (->> (map stores-info (drivers state))
         (reduce #(merge-with merge %1 %2) {})))


  (close
    [_]
    (when-let [errors (->> (drivers state)
                           (map #(try (do (close %) nil)
                                      (catch Exception e e)))
                           doall
                           (filter some?)
                           seq)]
      (throw (ex-info "Closing topologies failed."
                      {:topology root-application-id
                       :errors errors})))))

(defn driver
  "Creates a driver from the driver-supplier function, for each new partition
  encountered in order to test the locality of data."
  [application-id driver-supplier opts]
  (let [state (atom {:epoch (:initial-ms opts)})
        supplier #(driver-supplier application-id
                                   (partitioned-id %)
                                   (assoc opts
                                          :initial-ms (:epoch @state)))]
    ;; ensure at least one topo is made, in case the first thing we do is an advance time
    (driver! state :default supplier)
    (->PartitioningDriver opts state application-id supplier)))

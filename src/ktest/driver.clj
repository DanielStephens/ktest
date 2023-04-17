(ns ktest.driver
  (:require [ktest.batch-drivers.batching-driver :as batching]
            [ktest.batch-drivers.clean-output-driver :as clean]
            [ktest.batch-drivers.recursive-driver :as recurse]
            [ktest.batch-drivers.shuffle-driver :as shuffle]
            [ktest.drivers.combined-driver :as combi]
            [ktest.drivers.completing-internals-driver :as i-recurse]
            [ktest.drivers.partitioned-driver :as ptition]
            [ktest.drivers.topology-driver :as topo]
            [ktest.utils :refer :all]))

(defn partitioned-driver
  [opts group-id app-id topology-supplier]
  (let [group-app-id (str group-id "-" app-id)
        shared-topo-supplier (constantly (topology-supplier))
        driver-supplier (fn [app-id ptition-id opts]
                          (topo/driver app-id
                                       ptition-id
                                       shared-topo-supplier
                                       opts))]
    (-> (ptition/driver group-app-id
                        driver-supplier
                        opts)
        (i-recurse/driver opts))))

(defn combi-driver
  [opts name-topology-supplier-pairs]
  (let [group-id (genid)
        partitioned-drivers (->> name-topology-supplier-pairs
                                 (map (fn [[app-id topo-supplier]]
                                        (partitioned-driver opts
                                                            group-id
                                                            app-id
                                                            topo-supplier))))]
    (combi/driver partitioned-drivers opts)))

(defn default-driver
  [opts name-topology-supplier-pairs]
  (cond-> (combi-driver opts name-topology-supplier-pairs)
    true (batching/batch-driver)
    (:shuffle opts) (shuffle/batch-driver opts)
    (:recurse opts) (recurse/batch-driver opts)
    true (clean/batch-driver)))

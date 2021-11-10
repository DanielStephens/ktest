(ns ktest.drivers.combined-driver
  (:require [ktest.protocols.driver :refer :all]
            [ktest.drivers.partitioned-driver :as tg]
            [ktest.utils :refer :all]))

(defrecord CombiningDriver
  [current-epoch-millis opts drivers]
  Driver
  (pipe-input [_ topic message]
    (->> drivers
         (map (fn [d] (pipe-input d topic message)))
         (munge-outputs)))
  (advance-time [_ advance-millis]
    (swap! current-epoch-millis + advance-millis)
    (->> drivers
         (map (fn [d] (advance-time d advance-millis)))
         (munge-outputs)))
  (current-time [_]
    @current-epoch-millis)
  (close [_] (when-let [errors (->> drivers
                                    (map #(try (do (close %) nil)
                                               (catch Exception e e)))
                                    doall
                                    (filter some?)
                                    seq)]
               (throw (ex-info "Closing topologies failed."
                               {:errors errors})))))

(defn driver
  "Combines together multiple drivers, sending inputs to all of them"
  [drivers opts]
  (->CombiningDriver (atom (:initial-ms opts)) opts drivers))


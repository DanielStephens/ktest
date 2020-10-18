(ns ktest.driver)

(defprotocol Driver
  (pipe-raw-input [this topic message])
  (advance-time [this advance-millis])
  (close [_]))

(ns ktest.protocols.driver)

(defprotocol Driver

  (pipe-input [this topic message])

  (advance-time [this advance-millis])

  (current-time [this])

  (stores-info [this])

  (close [_]))

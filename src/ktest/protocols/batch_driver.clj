(ns ktest.protocols.batch-driver)

(defprotocol BatchDriver
  (pipe-inputs [this messages])
  (advance-time [this advance-millis])
  (current-time [this])
  (close [this]))

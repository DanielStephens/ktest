# ktest

[![Clojars Project](https://img.shields.io/clojars/v/djs/ktest.svg)](https://clojars.org/djs/ktest)

A clojure library to provide improved unit testing capabilities for your kafka topologies.

This project is very new and might have some way to go before being stable, the versioning may be missleading here as I have for now opted to version this project inline with the kafka version supported.

## Why not use TopologyTestDriver?

The main problem with TopologyTestDriver is that it only handles a single partition. This means if you have any code that steps outside of pure KafkaStreams logic, such as by using transform or process functions, you immediately loose the ability to test the locality of your data.

Ktest provides a way to supply custom partitioning, by default this is set up such that each key is a new partition, and ensures the locality of store data within those partitions.

You can swap in your own partitioning strategy to also test your code on a single partition if desired to understand data clashes as well.

## Why not use an embedded kafka?

While testing against a real kafka has many benefits for integration testing, it is not an appropriate tool for unit testing due to speed and lack of deterministic results.

Ktest is backed by TopologyTestDriver and certainly is still in need of performance improvements but regardless of that it is very easy to get setup with and start running quick tests.

Ontop of that, ktest provides a way to introduce deterministic randomness, you can opt in to deterministically shuffle the inputs to a topology before processing to catch those timing issues you might not have expected.

# Build & Test

The library has a few java classes in the original kafka namespaces, mostly just to provide public accessors to internal methods (although `CapturingStreamTask` also provides some functionality). As a result, it can't be run in a Clojure repl until the java classes have been build.

To build the java classes (and run the tests), run the `test.sh` script. Once the java classes have been built, you can run the clojure code and tests in a repl as normal, although of course any change to the java classes will require you to rerun the script and restart your repl.
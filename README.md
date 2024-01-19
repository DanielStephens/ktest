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

## Build & Test

The library has a few java classes in the original kafka namespaces, mostly just to provide public accessors to internal methods (although `CapturingStreamTask` also provides some functionality). As a result, it can't be run in a Clojure repl until the java classes have been build.

To build the java classes, run `clj -M:build`. If you want to build the java classes _and_ run the tests, run the `test.sh` script.

Once the java classes have been built, you can run the clojure code and tests in a repl as normal, although of course any change to the java classes will require you to rebuild the java classes and restart your repl.

## Library Structure - Drivers

The library's functionality is split across several nested drivers. These provide a certain amount of encapsulation between different features, but some features are spread across different drivers and it's important to understand which these are in order to understand how these features work

### The TopologyDriver

This driver wraps the `TopologyTestDriver`. Its main functionality is to return any messages output as a result of the input, and prevent publishing/propagation of certain messages through the system to provide an extension point for other drivers.

It sets up the `TopologyTestDriver` with a `CapturingStreamTask`, which wraps the original `StreamTask`, with the only addition being that it calls a delegate (in practice always `default-capture`) when the `addRecords` method is called instead of deferring to its inner `StreamTask`

`default-capture` treats the first source message and most internal messages normally (by deferring to `addRecords` the inner `StreamTask`), but it prevents repartition and subsequent source messages from being processed (as well as capturing repartition messages so they can be returned).

The idea behind this is that although the source and repartition messages produced by the topology would normally be processed by the test driver, we want to add a hook in to change the way they're processed. So we prevent them from being processed automatically, returning them instead, and then make sure they are processed by feeding them back into the system, which allows for them to be manipulated by an intermediary layer. 

In the case of the repartition messages, we want them to cause a repartition in our system mock of the partitions, so we catch them and feed them back into the system using the `CompletingInternalsDriver`, which allows them to be separated onto different partitions by the `PartitioningDriver`.

In the case of the source messages, we want them to get the opportunity to be shuffled and read in a different order, so we catch them and feed them back into the system using the `RecursionDriver`, which allows them to be reordered by the `ShuffleDriver` 

The `TopologyDriver` then returns a map of topic name (for non-repartition topics) or topic properties map (for repartition topics) to a list of messages published to that topic.

### The PartitioningDriver

This driver (in the `partitioned-driver` namespace) maintains a collection of `TopologyDriver`s and a fn (defined through the options argument but set by default to be the message key round-tripped through the serialiser) which generates a mock partition key. Every time the `PartitioningDriver` receives a new message, it forwards it to the `TopologyDriver` corresponding to the message's key, creating a new one if it has never seen that key before.

The idea is that this simulates the partitions being on separate nodes, since each message key corresponds to a different driver, so will be processed entirely independently.

### The CompletingInternalsDriver

This driver wraps a `PartitioningDriver`. When it receives a new message, it passes it along to the `PartitioningDriver`. It then feeds any repartitioning messages in the output back into the `PartitioningDriver`, repeating the process until there is no more output. Along the way it collects any non-repartitioning output, and returns it at the end. This allows the `PartitioningDriver` to allocate work to a new "partition" whenever the system switches to a new partition with a repartitioning message.

### The CombiningDriver

This driver can be found in the `combined-driver` namespace. It receives a collection of name-value pairs of topologies, and maintains a `CompletingInternalsDriver` for each one. Every time it's asked to publish a message, it publishes it to each topology's `CompletingInternalsDriver`, and every time it returns messages, it amalgamates the results from each topology's `CompletingInternalsDriver`. This allows us to simulate a system with multiple topologies rather than just one.

### The ApplyingSerdeDriver

This driver (in the `serde-driver` namespace) allows clients to deal with standard clojure maps. It delegates to a `CombiningDriver`, serialising all messages on the way in, and deserialising all messages on the way out.

### The BatchUpDriver

The `BatchUpDriver` lives in the `batching-driver` namespace. It fulfils the `BatchDriver` interface, rather than the `Driver` interface. The only difference between the two is that the `pipe-input` method on the `Driver` interface takes a topic and a single message, while the same method on the `BatchDriver` interface takes a sequence of messages (all of which must have the `:topic` field).

As you'd expect, the `BatchUpDriver` delegates to an `ApplyingSerdeDriver`, iterating over each message, and passing in the `:topic` field separately.

### The ShuffleDriver

Receives a collection of messages, and randomly reorders them (keeping the things on the same topic & partition in the same order), simulating the fact that there is no ordering between partitions. Obviously this has to sit outside the `BatchUpDriver` since it fulfils the `BatchDriver` rather than the `Driver` interface - it needs to receive a whole batch of messages at once so that it can shuffle them, which would be impossible if it received them one by one.

### The RecursionDriver

Receives the output from the previous stages and puts them back into the system, repeating until no more output is produced. This allows the `ShuffleDriver` the opportunity to reorder different waves of messages, simulating the fact that messages on different partitions may be read in any order.

### The CleaningBatchDriver

Removes any internal information from the output, reducing each message to a key/value map.

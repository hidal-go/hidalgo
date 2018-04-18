## Hierarchical key-value store

One of the most basic abstractions over a database index - a store that associates a composite
binary key with a single binary value.

This abstraction assumes that store can isolate key-value space into named hierarchy of "buckets",
hence the key is composed of names of these buckets (a path).

**Supported backends:**

* [Bolt](github.com/boltdb/bolt)

* [Flat KV](./kv-flat.md)
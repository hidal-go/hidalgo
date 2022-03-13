## Hierarchical key-value store

One of the most basic abstractions for a database index - a store that associates a composite
binary key with a single binary value.

This abstraction assumes that store can isolate key-value space into named hierarchy of "buckets",
hence the key is composed of names of these buckets (a path).

By storing a schema and using row serialization, these stores can implement [tuple store](./tuple-strict.md).

**Supported backends:**

* [Bolt](https://github.com/boltdb/bolt)
* [BBolt](https://github.com/coreos/bbolt)

* Emulated over [Flat KV](./kv-flat.md)

**Backend features:**

| Backend | Persistence | Concurrency | Transactions |
|---------|-------------|-------------|--------------|
| Bolt    | X           | X           | X            |
| BBolt   | X           | X           | X            |
## Flat key-value store

Flat KV store is the most basic abstraction - a store that associates a single binary key with a single binary value.

By using a specific key separator these stores can implement [hierarchical key-value store](./kv-hierarchical.md).

**Supported backends:**

* In-memory [B-Tree](github.com/cznic/b)

* [LevelDB](github.com/syndtr/goleveldb)

* [Badger](github.com/dgraph-io/badger)
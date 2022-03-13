## Flat key-value store

Flat KV store is the most basic abstraction - a store that associates a single binary key with a single binary value.

By using a specific key separator these stores can implement [hierarchical key-value store](./kv-hierarchical.md).

**Supported backends:**

* In-memory [B-Tree](https://github.com/cznic/b)

* [Badger](https://github.com/dgraph-io/badger)

* [Pebble](https://github.com/cockroachdb/pebble) (experimental)

* [LevelDB](https://github.com/syndtr/goleveldb)

* Downgrade of [Hierarchical KV](./kv-hierachical.md)

* Downgrade of [Tuple store](./tuple-strict.md)

**Backend features:**

| Backend | Persistence | Transactions |
|---------|-------------|--------------|
| B-Tree  | -           | X            |
| Badger  | X           | X            |
| Pebble  | X           | -            |
| LevelDB | X           | X            |

# Hierarchical key-value store

[![Go Reference](https://pkg.go.dev/badge/github.com/hidal-go/hidalgo/kv.svg "GoDoc for hierarchical key-value store within HiDAL-Go")](https://pkg.go.dev/github.com/hidal-go/hidalgo/kv)

One of the most basic abstractions for a database index - a store that associates a composite
binary key with a single binary value.

This abstraction assumes that store can isolate key-value space into named hierarchy of "buckets",
hence the key is composed of names of these buckets (a path).

By storing a schema and using row serialization, these stores can implement [tuple store](tuple-strict.md).

## Supported backends

* [Bolt](https://github.com/boltdb/bolt)
* [BBolt](https://github.com/coreos/bbolt)
* Emulated over [Flat KV](kv-flat.md)

## Backend features

| Backend               | Persistence | Concurrency | Transactions |
|-----------------------|-------------|-------------|--------------|
| Bolt                  | X           | X           | X            |
| BBolt                 | X           | X           | X            |
| [Flat KV](kv-flat.md) | X           | X           | X            |

## Backend optimizations

| Backend               | Seek | Prefix |
|-----------------------|------|--------|
| Bolt                  | X    | X      |
| BBolt                 | X    | X      |
| [Flat KV](kv-flat.md) | X    | X      |

## Notes

* Even though all backends expose `Tx` interface, some may behave incorrectly
  during concurrent writes to the same key. This is why transactions support
  may be marked unavailable for some backends. Contributions welcome :)
* Support for any of the features in meta-backends like flat KV store will depend
  on when the underlying backend support.
* Some features may be marked as not implemented for meta backend in this table,
  which means that they will not yet work for any of the underlying backends.

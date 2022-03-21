## Strict tuple store

[![Go Reference](https://pkg.go.dev/badge/github.com/hidal-go/hidalgo/tuple.svg "GoDoc for strict tuple store within HiDAL-Go")](https://pkg.go.dev/github.com/hidal-go/hidalgo/tuple)

This abstraction is based on tuple stores with a predefined schema. It should be able to isolate tuples of one type
into separate tables, and support lookup and scans on primary key. No secondary keys are supported.

By creating a simple key-value tables, these stores can implement [hierarchical key-value store](kv-hierarchical.md).

**Supported backends:**

* [SQL](sql-tuple.md) (meta-backend)
* [Google Datastore](https://cloud.google.com/datastore/)

* Emulated over [Hierarchical KV](kv-hierarchical.md)

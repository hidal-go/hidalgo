## Strict tuple store

This abstraction is based on tuple stores with a predefined schema. It should be able to isolate tuples of one type
into separate tables, and support lookup and scans on primary key. No secondary keys are supported.

**Supported backends:**

* [SQL](./sql-tuple.md)

* [Google Datastore](https://cloud.google.com/datastore/)

* Emulated over [Hierarchical KV](./kv-hierarchical.md)
package all

import (
	_ "github.com/hidal-go/hidalgo/kv/flat/badger"
	_ "github.com/hidal-go/hidalgo/kv/flat/btree"
	_ "github.com/hidal-go/hidalgo/kv/flat/leveldb"
	_ "github.com/hidal-go/hidalgo/kv/flat/pebble"
)

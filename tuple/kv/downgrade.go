package tuplekv

import (
	"context"
	"fmt"

	"github.com/hidal-go/hidalgo/filter"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/values"
)

func NewKV(ctx context.Context, db tuple.Store, table string) (flat.KV, error) {
	tx, err := db.Tx(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	_, err = tx.Table(ctx, table)
	if err == tuple.ErrTableNotFound {
		_, err = tx.CreateTable(ctx, tuple.Header{
			Name: table,
			Key: []tuple.KeyField{
				{Name: "key", Type: values.BytesType{}},
			},
			Data: []tuple.Field{
				{Name: "val", Type: values.BytesType{}},
			},
		})
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	return &flatKV{db: db, table: table}, nil
}

type flatKV struct {
	db    tuple.Store
	table string
}

func (kv *flatKV) Close() error {
	return kv.db.Close()
}

func (kv *flatKV) Tx(ctx context.Context, rw bool) (flat.Tx, error) {
	tx, err := kv.db.Tx(ctx, rw)
	if err != nil {
		return nil, err
	}
	tbl, err := tx.Table(ctx, kv.table)
	if err != nil {
		tx.Close()
		return nil, err
	}
	return &flatTx{tx: tx, tbl: tbl}, nil
}

func (kv *flatKV) View(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.View(ctx, kv, fn)
}

func (kv *flatKV) Update(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.Update(ctx, kv, fn)
}

type flatTx struct {
	tx  tuple.Tx
	tbl tuple.Table
}

func (tx *flatTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *flatTx) Close() error {
	return tx.tx.Close()
}

func flatKeyPart(b flat.Key) values.Bytes {
	return values.Bytes(b.Clone())
}

func flatKey(b flat.Key) tuple.Key {
	if b == nil {
		return nil
	}
	return tuple.Key{flatKeyPart(b)}
}

func flatData(b flat.Value) tuple.Data {
	return tuple.Data{values.Bytes(b.Clone())}
}

func (tx *flatTx) Get(ctx context.Context, key flat.Key) (flat.Value, error) {
	row, err := tx.tbl.GetTuple(ctx, flatKey(key))
	if err == tuple.ErrNotFound {
		return nil, flat.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	b, ok := row[0].(values.Bytes)
	if !ok || b == nil {
		return nil, fmt.Errorf("unexpected value type: %T", row[0])
	}
	return flat.Value(b), nil
}

func (tx *flatTx) GetBatch(ctx context.Context, key []flat.Key) ([]flat.Value, error) {
	keys := make([]tuple.Key, 0, len(key))
	for _, k := range key {
		keys = append(keys, flatKey(k))
	}
	rows, err := tx.tbl.GetTupleBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	vals := make([]flat.Value, len(key))
	for i, d := range rows {
		if d == nil {
			continue
		}
		b, ok := d[0].(values.Bytes)
		if !ok || b == nil {
			return nil, fmt.Errorf("unexpected value type: %T", d[0])
		}
		vals[i] = flat.Value(b)
	}
	return vals, nil
}

func (tx *flatTx) Put(ctx context.Context, k flat.Key, v flat.Value) error {
	return tx.tbl.UpdateTuple(ctx, tuple.Tuple{
		Key:  flatKey(k),
		Data: flatData(v),
	}, &tuple.UpdateOpt{Upsert: true})
}

func (tx *flatTx) Del(ctx context.Context, k flat.Key) error {
	return tx.tbl.DeleteTuples(ctx, &tuple.Filter{
		KeyFilter: tuple.Keys{flatKey(k)},
	})
}

func (tx *flatTx) Scan(ctx context.Context, opts ...flat.IteratorOption) flat.Iterator {
	tit := &flatIterator{ctx: ctx, tx: tx}
	tit.seek(ctx, nil)
	var it flat.Iterator = tit
	it = flat.ApplyIteratorOptions(it, opts)
	return it
}

var (
	_ flat.Seeker         = &flatIterator{}
	_ flat.PrefixIterator = &flatIterator{}
)

type flatIterator struct {
	ctx  context.Context
	tx   *flatTx
	pref flat.Key
	it   tuple.Iterator
	err  error
}

func (it *flatIterator) Reset() {
	it.err = nil
	if it.it != nil {
		it.it.Reset()
	}
}

func (it *flatIterator) WithPrefix(pref flat.Key) flat.Iterator {
	it.pref = pref
	it.seek(it.ctx, nil)
	return it
}

func (it *flatIterator) Close() error {
	return it.it.Close()
}

func (it *flatIterator) Err() error {
	if err := it.it.Err(); err != nil {
		return err
	}
	return it.err
}

func (it *flatIterator) filters() tuple.KeyFilters {
	if len(it.pref) == 0 {
		return nil
	}
	return tuple.KeyFilters{
		filter.Prefix(flatKeyPart(it.pref)),
	}
}

func (it *flatIterator) seek(ctx context.Context, key flat.Key) {
	it.Reset()
	if it.it != nil {
		_ = it.it.Close()
	}
	filters := it.filters()
	if len(key) != 0 {
		filters = append(filters, filter.GTE(flatKeyPart(key)))
	}
	var f *tuple.Filter
	if len(filters) != 0 {
		f = &tuple.Filter{KeyFilter: filters}
	}
	it.it = it.tx.tbl.Scan(ctx, &tuple.ScanOptions{
		Sort:   tuple.SortAsc,
		Filter: f,
	})
}

func (it *flatIterator) Seek(ctx context.Context, key flat.Key) bool {
	it.seek(ctx, key)
	return it.it.Next(ctx)
}

func (it *flatIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	return it.it.Next(ctx)
}

func (it *flatIterator) Key() flat.Key {
	key := it.it.Key()
	if len(key) == 0 {
		return nil
	} else if len(key) > 1 {
		it.err = fmt.Errorf("unexpected key size: %d", len(key))
		return nil
	}
	b, ok := key[0].(values.Bytes)
	if !ok || b == nil {
		it.err = fmt.Errorf("unexpected key type: %T", key[0])
		return nil
	}
	return flat.Key(b).Clone()
}

func (it *flatIterator) Val() flat.Value {
	data := it.it.Data()
	if len(data) == 0 {
		return nil
	}
	b, ok := data[0].(values.Bytes)
	if !ok || b == nil {
		it.err = fmt.Errorf("unexpected value type: %T", data[0])
		return nil
	}
	return flat.Value(b).Clone()
}

package tuplekv

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hidal-go/hidalgo/filter"
	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/tuplepb"
	"github.com/hidal-go/hidalgo/values"
)

func New(kv kv.KV) tuple.Store {
	return &tupleStore{db: kv}
}

type tupleStore struct {
	db kv.KV
}

func (db *tupleStore) Close() error {
	return db.db.Close()
}

func (db *tupleStore) Tx(rw bool) (tuple.Tx, error) {
	tx, err := db.db.Tx(rw)
	if err != nil {
		return nil, err
	}
	return &tupleTx{tx: tx}, nil
}

type tupleTx struct {
	tx kv.Tx
}

func (tx *tupleTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *tupleTx) Close() error {
	return tx.tx.Close()
}

func (tx *tupleTx) tableSchema(name string) kv.Key {
	k := kv.SKey("system", "table")
	if name != "" {
		k = k.AppendBytes([]byte(name))
	}
	return k
}

func (tx *tupleTx) Table(ctx context.Context, name string) (tuple.Table, error) {
	data, err := tx.tx.Get(ctx, tx.tableSchema(name))
	if err == kv.ErrNotFound {
		return nil, tuple.ErrTableNotFound
	} else if err != nil {
		return nil, err
	}
	h, err := tuplepb.UnmarshalTable(data)
	if err != nil {
		return nil, err
	}
	// TODO: cache into tx
	return &tupleTable{tx: tx, h: *h}, nil
}

func (tx *tupleTx) ListTables(ctx context.Context) ([]tuple.Table, error) {
	it := tx.tx.Scan(tx.tableSchema(""))
	defer it.Close()
	if err := it.Err(); err != nil {
		return nil, err
	}
	var tables []tuple.Table
	for it.Next(ctx) {
		h, err := tuplepb.UnmarshalTable(it.Val())
		if err != nil {
			return tables, err
		}
		tables = append(tables, &tupleTable{tx: tx, h: *h})
	}
	return tables, nil
}

func (tx *tupleTx) CreateTable(ctx context.Context, table tuple.Header) (tuple.Table, error) {
	if err := table.Validate(); err != nil {
		return nil, err
	}
	key := tx.tableSchema(table.Name)
	_, err := tx.tx.Get(ctx, key)
	if err == nil {
		return nil, tuple.ErrExists
	} else if err != nil && err != kv.ErrNotFound {
		return nil, err
	}
	data, err := tuplepb.MarshalTable(&table)
	if err != nil {
		return nil, err
	}
	err = tx.tx.Put(key, data)
	if err != nil {
		return nil, err
	}
	return &tupleTable{tx: tx, h: table.Clone()}, nil
}

type tupleTable struct {
	tx *tupleTx
	h  tuple.Header
}

func (tbl *tupleTable) schema() kv.Key {
	return tbl.tx.tableSchema(tbl.h.Name)
}

func toKvKey(k tuple.Key) kv.Key {
	if k == nil {
		return nil
	}
	key := make(kv.Key, 0, len(k))
	for _, s := range k {
		if s == nil {
			key = append(key, nil)
		} else {
			data, _ := s.MarshalSortable()
			key = append(key, data)
		}
	}
	return key
}

func (tbl *tupleTable) row(key tuple.Key) kv.Key {
	k := kv.SKey("data", "table", tbl.h.Name)
	if key != nil {
		k = k.Append(toKvKey(key))
	}
	return k
}

func (tbl *tupleTable) Drop(ctx context.Context) error {
	if err := tbl.Clear(ctx); err != nil {
		return err
	}
	return tbl.tx.tx.Del(tbl.schema())
}

func (tbl *tupleTable) Clear(ctx context.Context) error {
	// TODO: support prefix delete on kv
	it := tbl.tx.tx.Scan(tbl.row(nil))
	defer it.Close()
	for it.Next(ctx) {
		if err := tbl.tx.tx.Del(it.Key()); err != nil {
			return err
		}
	}
	return it.Err()
}

func (tbl *tupleTable) decodeKey(key kv.Key) (tuple.Key, error) {
	k0 := key
	pref := tbl.row(nil)
	key = key[len(pref):]
	if len(key) != len(tbl.h.Key) {
		return nil, fmt.Errorf("decodeKey: wrong key size: %d vs %d (%v)", len(key), len(tbl.h.Key), k0)
	}
	row := make(tuple.Key, len(tbl.h.Key))
	for i, f := range tbl.h.Key {
		v := f.Type.NewSortable()
		err := v.UnmarshalSortable(key[i])
		if err != nil {
			return nil, fmt.Errorf("cannot decode tuple key: %v", err)
		}
		row[i] = v.Sortable()
	}
	return row, nil
}
func (tbl *tupleTable) encodeTuple(data tuple.Data) (kv.Value, error) {
	fields := make([][]byte, len(data))
	sz := 0
	for i, v := range data {
		b, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		fields[i] = b
		// TODO: calculate size more precisely
		sz += len(b) + binary.MaxVarintLen32
	}
	buf := make(kv.Value, sz)
	i := 0
	for _, f := range fields {
		i += binary.PutUvarint(buf[i:], uint64(len(f)))
		i += copy(buf[i:], f)
	}
	buf = buf[:i]
	return buf, nil
}
func (tbl *tupleTable) decodeTuple(data kv.Value) (tuple.Data, error) {
	row := make(tuple.Data, len(tbl.h.Data))
	for i, f := range tbl.h.Data {
		v := f.Type.New()
		sz, n := binary.Uvarint(data)
		data = data[n:]
		if n == 0 {
			return nil, fmt.Errorf("cannot decode tuple data: %v", io.ErrUnexpectedEOF)
		} else if sz > uint64(len(data)) {
			return nil, fmt.Errorf("invalid tuple field size: %d vs %d", sz, len(data))
		}
		err := v.UnmarshalBinary(data[:sz])
		data = data[sz:]
		if err != nil {
			return nil, fmt.Errorf("cannot decode tuple field: %v", err)
		}
		row[i] = v.Value()
	}
	return row, nil
}

func (tbl *tupleTable) GetTuple(ctx context.Context, key tuple.Key) (tuple.Data, error) {
	if err := tbl.h.ValidateKey(key, false); err != nil {
		return nil, err
	}
	data, err := tbl.tx.tx.Get(ctx, tbl.row(key))
	if err == kv.ErrNotFound {
		return nil, tuple.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return tbl.decodeTuple(data)
}

func (tbl *tupleTable) GetTupleBatch(ctx context.Context, key []tuple.Key) ([]tuple.Data, error) {
	keys := make([]kv.Key, 0, len(key))
	for _, k := range key {
		if err := tbl.h.ValidateKey(k, false); err != nil {
			return nil, err
		}
		keys = append(keys, tbl.row(k))
	}
	data, err := tbl.tx.tx.GetBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	rows := make([]tuple.Data, len(key))
	for i, p := range data {
		if p == nil {
			continue
		}
		row, err := tbl.decodeTuple(p)
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return rows, nil
}

func (tbl *tupleTable) InsertTuple(ctx context.Context, t tuple.Tuple) (tuple.Key, error) {
	if err := tbl.h.ValidateKey(t.Key, true); err != nil {
		return nil, err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return nil, err
	}
	if tbl.h.Key[0].Auto {
		// FIXME: auto fields
		return nil, fmt.Errorf("auto fields are not yet supported")
	}
	key := tbl.row(t.Key)
	_, err := tbl.tx.tx.Get(ctx, key)
	if err == nil {
		return nil, tuple.ErrExists
	} else if err != nil && err != kv.ErrNotFound {
		return nil, err
	}
	val, err := tbl.encodeTuple(t.Data)
	if err != nil {
		return nil, err
	}
	err = tbl.tx.tx.Put(key, val)
	if err != nil {
		return nil, err
	}
	return t.Key, nil
}

func (tbl *tupleTable) UpdateTuple(ctx context.Context, t tuple.Tuple, opt *tuple.UpdateOpt) error {
	if err := tbl.h.ValidateKey(t.Key, false); err != nil {
		return err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return err
	}
	key := tbl.row(t.Key)
	if opt == nil {
		opt = &tuple.UpdateOpt{}
	}
	if !opt.Upsert {
		_, err := tbl.tx.tx.Get(ctx, key)
		if err == kv.ErrNotFound {
			return tuple.ErrNotFound
		} else if err != nil {
			return err
		}
	}
	val, err := tbl.encodeTuple(t.Data)
	if err != nil {
		return err
	}
	err = tbl.tx.tx.Put(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (tbl *tupleTable) DeleteTuples(ctx context.Context, f *tuple.Filter) error {
	if !f.IsAny() {
		// if we know the list of keys in advance
		if arr, ok := f.KeyFilter.(tuple.Keys); ok {
			// check keys against the schema
			for _, key := range arr {
				if err := tbl.h.ValidateKey(key, false); err != nil {
					return err
				}
			}
			if f.IsAnyData() {
				// if data won't be filtered - delete tuples directly
				for _, key := range arr {
					if err := tbl.tx.tx.Del(tbl.row(key)); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	// fallback to iterate + delete
	it := tbl.scan(f)
	defer it.Close()
	for it.Next(ctx) {
		if err := tbl.tx.tx.Del(it.key()); err != nil {
			return err
		}
	}
	return it.Err()
}

func (tbl *tupleTable) scan(f *tuple.Filter) *tupleIterator {
	pref := tbl.row(nil)
	if !f.IsAny() {
		if kf, ok := f.KeyFilter.(tuple.KeyFilters); ok {
			// find common prefix, if any
		loop:
			for _, vf := range kf {
				switch vf := vf.(type) {
				case filter.Equal:
					s, ok := vf.Value.(values.Sortable)
					if !ok {
						break loop
					}
					pref = pref.Append(toKvKey(tuple.Key{s}))
				case filter.Range:
					p, ok := vf.Prefix()
					if ok && p != nil {
						pref = pref.Append(toKvKey(tuple.Key{p}))
					}
					break loop
				}
			}
		}
	}
	return &tupleIterator{
		tbl: tbl, f: f,
		it: tbl.tx.tx.Scan(pref),
	}
}

func (tbl *tupleTable) Scan(sorting tuple.Sorting, f *tuple.Filter) tuple.Iterator {
	if sorting == tuple.SortDesc {
		// FIXME: support descending order
		return &tupleIterator{err: fmt.Errorf("descending order is not supported yet")}
	}
	return tbl.scan(f)
}

type tupleIterator struct {
	tbl *tupleTable
	f   *tuple.Filter
	it  kv.Iterator
	err error
}

func (it *tupleIterator) Close() error {
	if it.it != nil {
		return it.it.Close()
	}
	return nil
}

func (it *tupleIterator) Err() error {
	if it.it == nil {
		return it.err
	}
	if err := it.it.Err(); err != nil {
		return err
	}
	return it.err
}

func (it *tupleIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	return tuple.FilterIterator(it, it.f, func() bool {
		return it.it.Next(ctx)
	})
}

func (it *tupleIterator) key() kv.Key {
	if it.it == nil {
		return nil
	}
	return it.it.Key()
}
func (it *tupleIterator) Key() tuple.Key {
	if it.it == nil {
		return nil
	}
	data, err := it.tbl.decodeKey(it.key())
	if err != nil {
		it.err = err
	}
	return data
}

func (it *tupleIterator) Data() tuple.Data {
	if it.it == nil {
		return nil
	}
	data, err := it.tbl.decodeTuple(it.it.Val())
	if err != nil {
		it.err = err
	}
	return data
}

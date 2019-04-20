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

func tupleErr(err error) error {
	switch err {
	case kv.ErrNotFound:
		return tuple.ErrNotFound
	case kv.ErrReadOnly:
		return tuple.ErrReadOnly
	}
	return err
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

func (db *tupleStore) tableSchema(name string) kv.Key {
	k := kv.SKey("system", "table")
	if name != "" {
		k = k.AppendBytes([]byte(name))
	}
	return k
}

func (db *tupleStore) tableAuto(name string) kv.Key {
	k := kv.SKey("system", "auto")
	if name != "" {
		k = k.AppendBytes([]byte(name))
	}
	return k
}

func (db *tupleStore) tableWith(ctx context.Context, tx kv.Tx, name string) (*tupleTableInfo, error) {
	// TODO: cache
	data, err := tx.Get(ctx, db.tableSchema(name))
	if err == kv.ErrNotFound {
		return nil, tuple.ErrTableNotFound
	} else if err != nil {
		return nil, tupleErr(err)
	}
	h, err := tuplepb.UnmarshalTable(data)
	if err != nil {
		return nil, err
	}
	return &tupleTableInfo{h: *h}, nil
}

func (db *tupleStore) Table(ctx context.Context, name string) (tuple.TableInfo, error) {
	tx, err := db.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()

	info, err := db.tableWith(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (db *tupleStore) listTablesWith(ctx context.Context, tx kv.Tx) ([]*tupleTableInfo, error) {
	it := tx.Scan(db.tableSchema(""))
	defer it.Close()
	if err := it.Err(); err != nil {
		return nil, tupleErr(err)
	}
	var tables []*tupleTableInfo
	for it.Next(ctx) {
		h, err := tuplepb.UnmarshalTable(it.Val())
		if err != nil {
			return tables, err
		}
		tables = append(tables, &tupleTableInfo{h: *h})
	}
	return tables, nil
}

func (db *tupleStore) ListTables(ctx context.Context) ([]tuple.TableInfo, error) {
	tx, err := db.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()

	tables, err := db.listTablesWith(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := make([]tuple.TableInfo, 0, len(tables))
	for _, t := range tables {
		out = append(out, t)
	}
	return out, nil
}

var _ tuple.TableInfo = (*tupleTableInfo)(nil)

type tupleTableInfo struct {
	h tuple.Header
}

func (t *tupleTableInfo) Header() tuple.Header {
	return t.h.Clone()
}

func (t *tupleTableInfo) Open(tx tuple.Tx) (tuple.Table, error) {
	ktx, ok := tx.(*tupleTx)
	if !ok {
		return nil, fmt.Errorf("tuplekv: unexpected tx type: %T", tx)
	}
	return &tupleTable{tx: ktx, h: t.h}, nil
}

type tupleTx struct {
	db *tupleStore
	tx kv.Tx
}

func (tx *tupleTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *tupleTx) Close() error {
	return tx.tx.Close()
}

func (tx *tupleTx) Table(ctx context.Context, name string) (tuple.Table, error) {
	info, err := tx.db.tableWith(ctx, tx.tx, name)
	if err != nil {
		return nil, err
	}
	return info.Open(tx)
}

func (tx *tupleTx) ListTables(ctx context.Context) ([]tuple.Table, error) {
	tables, err := tx.db.listTablesWith(ctx, tx.tx)
	if err != nil {
		return nil, err
	}
	out := make([]tuple.Table, 0, len(tables))
	for _, t := range tables {
		tbl, err := t.Open(tx)
		if err != nil {
			return nil, err
		}
		out = append(out, tbl)
	}
	return out, nil
}

func (tx *tupleTx) CreateTable(ctx context.Context, table tuple.Header) (tuple.Table, error) {
	if err := table.Validate(); err != nil {
		return nil, err
	}
	key := tx.db.tableSchema(table.Name)
	_, err := tx.tx.Get(ctx, key)
	if err == nil {
		return nil, tuple.ErrExists
	} else if err != nil && err != kv.ErrNotFound {
		return nil, tupleErr(err)
	}
	data, err := tuplepb.MarshalTable(&table)
	if err != nil {
		return nil, err
	}
	err = tx.tx.Put(key, data)
	if err != nil {
		return nil, tupleErr(err)
	}
	// TODO: populate table info cache on commit
	return &tupleTable{tx: tx, h: table.Clone()}, nil
}

type tupleTable struct {
	tx *tupleTx
	h  tuple.Header
}

func (tbl *tupleTable) Header() tuple.Header {
	return tbl.h.Clone()
}

func (tbl *tupleTable) Open(tx tuple.Tx) (tuple.Table, error) {
	return (&tupleTableInfo{h: tbl.h}).Open(tx)
}

func (tbl *tupleTable) schema() kv.Key {
	return tbl.tx.db.tableSchema(tbl.h.Name)
}

func (tbl *tupleTable) auto() kv.Key {
	return tbl.tx.db.tableAuto(tbl.h.Name)
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
	} else {
		k = k.Append(kv.Key{nil})
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
			return tupleErr(err)
		}
	}
	return it.Err()
}

func (tbl *tupleTable) decodeKey(key kv.Key) (tuple.Key, error) {
	k0 := key
	pref := tbl.row(nil)
	if n := len(pref); n != 0 && len(pref[n-1]) == 0 {
		pref = pref[:n-1]
	}
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
		return nil, tupleErr(err)
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
		return nil, tupleErr(err)
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

func (tbl *tupleTable) nextAuto(ctx context.Context) (tuple.Key, error) {
	key := tbl.auto()
	v, err := tbl.tx.tx.Get(ctx, key)
	if err == kv.ErrNotFound {
		// first - set to 0
		v = kv.Value{0}
	} else if err != nil {
		return nil, err
	}
	var last values.UInt
	if err = last.UnmarshalBinary(v); err != nil {
		return nil, err
	}
	last++
	data, err := last.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if err = tbl.tx.tx.Put(key, kv.Value(data)); err != nil {
		return nil, err
	}
	return tuple.Key{last}, nil
}

func (tbl *tupleTable) InsertTuple(ctx context.Context, t tuple.Tuple) (tuple.Key, error) {
	if err := tbl.h.ValidateKey(t.Key, true); err != nil {
		return nil, err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return nil, err
	}
	if tbl.h.Key[0].Auto {
		key, err := tbl.nextAuto(ctx)
		if err != nil {
			return nil, err
		}
		t.Key = key
	}
	key := tbl.row(t.Key)
	_, err := tbl.tx.tx.Get(ctx, key)
	if err == nil {
		return nil, tuple.ErrExists
	} else if err != nil && err != kv.ErrNotFound {
		return nil, tupleErr(err)
	}
	val, err := tbl.encodeTuple(t.Data)
	if err != nil {
		return nil, err
	}
	err = tbl.tx.tx.Put(key, val)
	if err != nil {
		return nil, tupleErr(err)
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
			return tupleErr(err)
		}
	}
	val, err := tbl.encodeTuple(t.Data)
	if err != nil {
		return err
	}
	err = tbl.tx.tx.Put(key, val)
	if err != nil {
		return tupleErr(err)
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
						return tupleErr(err)
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
			return tupleErr(err)
		}
	}
	return it.Err()
}

func (tbl *tupleTable) scan(f *tuple.Filter) *tupleIterator {
	pref := tbl.row(nil)
	removeWildcard := func() {
		if n := len(pref); n != 0 && len(pref[n-1]) == 0 {
			pref = pref[:n-1]
		}
	}
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
					removeWildcard()
					pref = pref.Append(toKvKey(tuple.Key{s}))
				case filter.Range:
					p, ok := vf.Prefix()
					if ok && p != nil {
						removeWildcard()
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

func (tbl *tupleTable) Scan(opt *tuple.ScanOptions) tuple.Iterator {
	if opt == nil {
		opt = &tuple.ScanOptions{}
	}
	if opt.Sort == tuple.SortDesc {
		// FIXME: support descending order
		return &tupleIterator{err: fmt.Errorf("descending order is not supported yet")}
	}
	// FIXME: support limit
	return tbl.scan(opt.Filter)
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

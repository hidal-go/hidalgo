package datastore

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/tuplepb"
	"github.com/hidal-go/hidalgo/values"
)

func Open(ctx context.Context, projectID string) (tuple.Store, error) {
	cli, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return OpenClient(cli), nil
}

func OpenClient(cli *datastore.Client) tuple.Store {
	return &TupleStore{c: cli}
}

type TupleStore struct {
	c *datastore.Client
}

func (s *TupleStore) Close() error {
	return s.c.Close()
}

func (s *TupleStore) metaRoot() *datastore.Key {
	return datastore.NameKey(kindHidalgo, idHidalgo, nil)
}

func (s *TupleStore) tableKey(name string) *datastore.Key {
	hi := s.metaRoot()
	return datastore.NameKey(kindTable, name, hi)
}

type tableInfo struct {
	h tuple.Header
}

func (t *tableInfo) Header() tuple.Header {
	return t.h.Clone()
}

func (t *tableInfo) Open(ctx context.Context, tx tuple.Tx) (tuple.Table, error) {
	dtx, ok := tx.(*Tx)
	if !ok {
		return nil, fmt.Errorf("datastore: unexpected tx type: %T", tx)
	}
	return &Table{tx: dtx, h: t.h}, nil
}

func (s *TupleStore) Table(ctx context.Context, name string) (tuple.TableInfo, error) {
	var t tableObject
	err := s.c.Get(ctx, s.tableKey(name), &t)
	if err == datastore.ErrNoSuchEntity {
		return nil, tuple.ErrTableNotFound
	} else if err != nil {
		return nil, err
	}
	h, err := tuplepb.UnmarshalTable(t.Data)
	if err != nil {
		return nil, err
	}
	return &tableInfo{h: *h}, nil
}

func (s *TupleStore) ListTables(ctx context.Context) ([]tuple.TableInfo, error) {
	q := datastore.NewQuery(kindTable).Ancestor(s.metaRoot())
	var tables []tableObject
	_, err := s.c.GetAll(ctx, q, &tables)
	if err != nil {
		return nil, err
	}
	out := make([]tuple.TableInfo, 0, len(tables))
	for _, t := range tables {
		h, err := tuplepb.UnmarshalTable(t.Data)
		if err != nil {
			return out, err
		}
		out = append(out, &tableInfo{h: *h})
	}
	return out, nil
}

func (s *TupleStore) Tx(ctx context.Context, rw bool) (tuple.Tx, error) {
	return &Tx{s: s, rw: rw}, nil
}

func (s *TupleStore) View(ctx context.Context, fn func(tx tuple.Tx) error) error {
	return tuple.View(ctx, s, fn)
}

func (s *TupleStore) Update(ctx context.Context, fn func(tx tuple.Tx) error) error {
	return tuple.Update(ctx, s, fn)
}

const (
	kindHidalgo = "_hidalgo"
	idHidalgo   = "tuple"
	kindTable   = "table"
)

type Tx struct {
	s  *TupleStore
	rw bool
}

func (tx *Tx) Commit(ctx context.Context) error {
	// TODO: support transactions properly
	return nil
}

func (tx *Tx) Close() error {
	return nil
}

func (tx *Tx) Table(ctx context.Context, name string) (tuple.Table, error) {
	info, err := tx.s.Table(ctx, name)
	if err != nil {
		return nil, err
	}
	return info.Open(ctx, tx)
}

func (tx *Tx) ListTables(ctx context.Context) ([]tuple.Table, error) {
	tables, err := tx.s.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]tuple.Table, 0, len(tables))
	for _, t := range tables {
		tbl, err := t.Open(ctx, tx)
		if err != nil {
			return out, err
		}
		out = append(out, tbl)
	}
	return out, nil
}

type tableObject struct {
	Data []byte `datastore:",noindex"`
}

func (tx *Tx) CreateTable(ctx context.Context, table tuple.Header) (tuple.Table, error) {
	if !tx.rw {
		return nil, tuple.ErrReadOnly
	}
	data, err := tuplepb.MarshalTable(&table)
	if err != nil {
		return nil, err
	}
	k := tx.s.tableKey(table.Name)
	_, err = tx.s.c.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var t tableObject
		err := tx.Get(k, &t)
		if err == nil {
			return tuple.ErrTableExists
		} else if err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
		t = tableObject{Data: data}
		_, err = tx.Put(k, &t)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Table{tx: tx, h: table}, nil
}

type Table struct {
	tx *Tx
	h  tuple.Header
}

func (tbl *Table) Header() tuple.Header {
	return tbl.h.Clone()
}

func (tbl *Table) Open(ctx context.Context, tx tuple.Tx) (tuple.Table, error) {
	return (&tableInfo{h: tbl.h}).Open(ctx, tx)
}

func (tbl *Table) cli() *datastore.Client {
	return tbl.tx.s.c
}

func (tbl *Table) Drop(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	if err := tbl.Clear(ctx); err != nil {
		return err
	}
	return tbl.cli().Delete(ctx, tbl.tx.s.tableKey(tbl.h.Name))
}

func (tbl *Table) Clear(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	for {
		q := datastore.NewQuery(tbl.h.Name).KeysOnly().Limit(100)
		keys, err := tbl.cli().GetAll(ctx, q, nil)
		if err != nil {
			return err
		} else if len(keys) == 0 {
			return nil
		}
		err = tbl.cli().DeleteMulti(ctx, keys)
		if err != nil {
			return err
		}
	}
}

func (tbl *Table) key(key tuple.Key, auto bool) *datastore.Key {
	kind := tbl.h.Name
	var k *datastore.Key
	for i, c := range tbl.h.Key {
		v := key[i]
		switch c.Type.(type) {
		case values.StringType:
			k = datastore.NameKey(kind, string(v.(values.String)), k)
		case values.IntType:
			k = datastore.IDKey(kind, int64(v.(values.Int)), k)
		case values.UIntType:
			if auto && c.Auto {
				k = datastore.IncompleteKey(kind, k)
			} else {
				k = datastore.IDKey(kind, int64(v.(values.UInt)), k)
			}
		default:
			d, err := v.MarshalSortable()
			if err != nil {
				panic(err)
			}
			k = datastore.NameKey(kind, hex.EncodeToString(d), k)
		}
	}
	return k
}

func (tbl *Table) parseKey(key *datastore.Key) (tuple.Key, error) {
	k := make(tuple.Key, len(tbl.h.Key))
	for i := len(k) - 1; i >= 0; i-- {
		if key == nil {
			return nil, fmt.Errorf("short key")
		}
		c := tbl.h.Key[i]
		switch c.Type.(type) {
		case values.StringType:
			k[i] = values.String(key.Name)
		case values.IntType:
			k[i] = values.Int(key.ID)
		case values.UIntType:
			k[i] = values.UInt(key.ID)
		default:
			d, err := hex.DecodeString(key.Name)
			if err != nil {
				return nil, err
			}
			v := c.Type.NewSortable()
			err = v.UnmarshalSortable(d)
			if err != nil {
				return nil, err
			}
			k[i] = v.Sortable()
		}
		key = key.Parent
	}
	return k, nil
}

var _ datastore.PropertyLoadSaver = (*payload)(nil)

type payload struct {
	h *tuple.Header
	t tuple.Tuple
}

func (p *payload) Load(props []datastore.Property) error {
	keys := false
	if p.t.Key == nil {
		keys = true
		p.t.Key = make(tuple.Key, len(p.h.Key))
	}
	p.t.Data = make(tuple.Data, len(p.h.Data))
	for _, f := range props {
		if f.Value == nil && !keys {
			continue
		}
		if keys {
			if c, i := p.h.KeyByName(f.Name); c != nil {
				val := f.Value
				var v values.Sortable
				switch c.Type.(type) {
				case values.BytesType:
					v = values.Bytes(val.([]byte))
				case values.StringType:
					v = values.String(val.(string))
				case values.IntType:
					v = values.Int(val.(int64))
				case values.UIntType:
					v = values.UInt(val.(int64))
				case values.BoolType:
					v = values.Bool(val.(bool))
				case values.TimeType:
					v = values.AsTime(val.(time.Time))
				default:
					d := c.Type.NewSortable()
					err := d.UnmarshalSortable(val.([]byte))
					if err != nil {
						return err
					}
					v = d.Sortable()
				}
				p.t.Key[i] = v
				continue
			}
		}
		if c, i := p.h.DataByName(f.Name); c != nil && f.Value != nil {
			val := f.Value
			var v values.Value
			switch c.Type.(type) {
			case values.BytesType:
				v = values.Bytes(val.([]byte))
			case values.StringType:
				v = values.String(val.(string))
			case values.IntType:
				v = values.Int(val.(int64))
			case values.UIntType:
				v = values.UInt(val.(int64))
			case values.FloatType:
				v = values.Float(val.(float64))
			case values.BoolType:
				v = values.Bool(val.(bool))
			case values.TimeType:
				v = values.AsTime(val.(time.Time))
			default:
				d := c.Type.New()
				err := d.UnmarshalBinary(val.([]byte))
				if err != nil {
					return err
				}
				v = d.Value()
			}
			p.t.Data[i] = v
		}
	}
	return nil
}

func (p *payload) Save() ([]datastore.Property, error) {
	out := make([]datastore.Property, 0, len(p.h.Key)+len(p.h.Data))
	for i, c := range p.h.Key {
		v := p.t.Key[i]
		var val interface{}
		if v != nil {
			switch c.Type.(type) {
			case values.BytesType:
				val = []byte(v.(values.Bytes))
			case values.StringType:
				val = string(v.(values.String))
			case values.IntType:
				val = int64(v.(values.Int))
			case values.UIntType:
				val = int64(v.(values.UInt))
			case values.BoolType:
				val = bool(v.(values.Bool))
			case values.TimeType:
				val = time.Time(v.(values.Time))
			default:
				data, err := v.MarshalSortable()
				if err != nil {
					return nil, err
				}
				val = data
			}
		}
		out = append(out, datastore.Property{
			Name:    c.Name,
			NoIndex: false,
			Value:   val,
		})
	}
	for i, c := range p.h.Data {
		v := p.t.Data[i]
		var val interface{}
		if v != nil {
			switch c.Type.(type) {
			case values.BytesType:
				val = []byte(v.(values.Bytes))
			case values.StringType:
				val = string(v.(values.String))
			case values.IntType:
				val = int64(v.(values.Int))
			case values.UIntType:
				val = int64(v.(values.UInt))
			case values.FloatType:
				val = float64(v.(values.Float))
			case values.BoolType:
				val = bool(v.(values.Bool))
			case values.TimeType:
				val = time.Time(v.(values.Time))
			default:
				data, err := v.MarshalBinary()
				if err != nil {
					return nil, err
				}
				val = data
			}
		}
		out = append(out, datastore.Property{
			Name:    c.Name,
			NoIndex: true,
			Value:   val,
		})
	}
	return out, nil
}

func (tbl *Table) GetTuple(ctx context.Context, key tuple.Key) (tuple.Data, error) {
	if err := tbl.h.ValidateKey(key, false); err != nil {
		return nil, err
	}
	p := &payload{h: &tbl.h}
	p.t.Key = key
	err := tbl.cli().Get(ctx, tbl.key(key, false), p)
	if err == datastore.ErrNoSuchEntity {
		return nil, tuple.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return p.t.Data, nil
}

func (tbl *Table) GetTupleBatch(ctx context.Context, keys []tuple.Key) ([]tuple.Data, error) {
	dkeys := make([]*datastore.Key, 0, len(keys))
	for _, k := range keys {
		if err := tbl.h.ValidateKey(k, false); err != nil {
			return nil, err
		}
		dkeys = append(dkeys, tbl.key(k, false))
	}
	data := make([]payload, len(keys))
	for i := range data {
		data[i].h = &tbl.h
	}
	err := tbl.cli().GetMulti(ctx, dkeys, data)
	if err != nil {
		return nil, err
	}
	out := make([]tuple.Data, len(keys))
	for i, d := range data {
		out[i] = d.t.Data
	}
	return out, nil
}

func (tbl *Table) InsertTuple(ctx context.Context, t tuple.Tuple) (tuple.Key, error) {
	if !tbl.tx.rw {
		return nil, tuple.ErrReadOnly
	} else if err := tbl.h.ValidateKey(t.Key, true); err != nil {
		return nil, err
	} else if err := tbl.h.ValidateData(t.Data); err != nil {
		return nil, err
	}
	tx, err := tbl.cli().NewTransaction(ctx)
	if err != nil {
		return nil, err
	}
	k := tbl.key(t.Key, true)
	if err := tx.Get(k, &payload{h: &tbl.h}); err == nil {
		tx.Rollback()
		return nil, tuple.ErrExists
	}
	pk, err := tx.Put(k, &payload{h: &tbl.h, t: t})
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	c, err := tx.Commit()
	if err != nil {
		return nil, err
	}
	if len(tbl.h.Key) == 0 || !tbl.h.Key[0].Auto {
		return t.Key, nil
	}
	id := c.Key(pk)
	return tuple.Key{values.UInt(id.ID)}, nil
}

func (tbl *Table) UpdateTuple(ctx context.Context, t tuple.Tuple, opt *tuple.UpdateOpt) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	} else if err := tbl.h.ValidateKey(t.Key, false); err != nil {
		return err
	} else if err := tbl.h.ValidateData(t.Data); err != nil {
		return err
	}
	if opt == nil {
		opt = &tuple.UpdateOpt{}
	}
	if opt.Upsert {
		_, err := tbl.cli().Put(ctx, tbl.key(t.Key, false), &payload{h: &tbl.h, t: t})
		return err
	}
	tx, err := tbl.cli().NewTransaction(ctx)
	if err != nil {
		return err
	}
	k := tbl.key(t.Key, false)
	if err := tx.Get(k, &payload{h: &tbl.h}); err == datastore.ErrNoSuchEntity {
		return tuple.ErrNotFound
	}
	_, err = tx.Put(k, &payload{h: &tbl.h, t: t})
	if err != nil {
		return err
	}
	_, err = tx.Commit()
	return err
}

func (tbl *Table) DeleteTuples(ctx context.Context, f *tuple.Filter) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	return tuple.DeleteEach(ctx, tbl, f)
}

func (tbl *Table) DeleteTuplesByKey(ctx context.Context, keys []tuple.Key) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	dkeys := make([]*datastore.Key, 0, len(keys))
	for _, k := range keys {
		if err := tbl.h.ValidateKey(k, false); err != nil {
			return err
		}
		dkeys = append(dkeys, tbl.key(k, false))
	}
	return tbl.cli().DeleteMulti(ctx, dkeys)
}

func (tbl *Table) Scan(ctx context.Context, opt *tuple.ScanOptions) tuple.Iterator {
	if opt == nil {
		opt = &tuple.ScanOptions{}
	}
	q := datastore.NewQuery(tbl.h.Name)
	if opt.KeysOnly {
		q = q.KeysOnly()
	}
	if opt.Limit > 0 {
		q = q.Limit(opt.Limit)
	}
	switch opt.Sort {
	case tuple.SortAsc:
		for _, f := range tbl.h.Key {
			q = q.Order(f.Name)
		}
	case tuple.SortDesc:
		for _, f := range tbl.h.Key {
			q = q.Order("-" + f.Name)
		}
	}
	return &Iterator{tbl: tbl, q: q, keysOnly: opt.KeysOnly, f: opt.Filter}
}

type Iterator struct {
	tbl      *Table
	q        *datastore.Query
	keysOnly bool
	f        *tuple.Filter

	it  *datastore.Iterator
	t   tuple.Tuple
	err error
}

func (it *Iterator) Reset() {
	it.t = tuple.Tuple{}
	it.err = nil
	if it.it != nil {
		it.it = nil
	}
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.it == nil {
		it.it = it.tbl.cli().Run(ctx, it.q)
	}
	return tuple.FilterIterator(it, it.f, func() bool {
		it.t = tuple.Tuple{}
		var (
			p   *payload
			key *datastore.Key
			dst interface{}
		)
		if !it.keysOnly {
			p = &payload{h: &it.tbl.h}
			p.t.Key = tuple.Key{} // disable key parsing
			dst = p
		}
		key, it.err = it.it.Next(dst)
		if it.err != nil {
			return false
		}
		k, err := it.tbl.parseKey(key)
		if err != nil {
			it.err = err
			return false
		}
		it.t = tuple.Tuple{Key: k}
		if p != nil {
			it.t.Data = p.t.Data
		}
		return true
	})
}

func (it *Iterator) Err() error {
	if it.err == iterator.Done {
		return nil
	}
	return it.err
}

func (it *Iterator) Close() error {
	return it.Err()
}

func (it *Iterator) Key() tuple.Key {
	return it.t.Key
}

func (it *Iterator) Data() tuple.Data {
	return it.t.Data
}

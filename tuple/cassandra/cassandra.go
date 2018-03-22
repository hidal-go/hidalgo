package cassandra

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/nwca/hidalgo/tuple"
	"github.com/nwca/hidalgo/types"
)

var (
	typeMap = map[types.Type]string{
		types.StringType{}: "text",
		types.BytesType{}:  "blob",
		types.IntType{}:    "bigint",
		types.BoolType{}:   "boolean",
		types.FloatType{}:  "double",
		types.TimeType{}:   "timestamp",
	}
	revTypeMap = make(map[string]types.Type)
)

func init() {
	for t, s := range typeMap {
		revTypeMap[s] = t
	}
}

func New(sess *gocql.Session, keyspace string) *DB {
	return &DB{sess: sess, ns: keyspace}
}

var _ tuple.Store = (*DB)(nil)

type DB struct {
	sess *gocql.Session
	ns   string
}

func (db *DB) Close() error {
	db.sess.Close()
	return nil
}

func (db *DB) Tx(rw bool) (tuple.Tx, error) {
	return &Tx{
		sess: db.sess, ns: db.ns,
		rw: rw,
	}, nil
}

type Tx struct {
	sess *gocql.Session
	ns   string
	rw   bool
}

func (tx *Tx) Commit(ctx context.Context) error {
	// TODO: at least try to buffer some data?
	return nil
}

func (tx *Tx) Close() error {
	return nil
}

func (tx *Tx) query(ctx context.Context, qu string, args ...interface{}) *gocql.Query {
	return tx.sess.Query(qu, args...).WithContext(ctx)
}

func (tx *Tx) queryf(ctx context.Context, format string, args ...interface{}) *gocql.Query {
	return tx.query(ctx, fmt.Sprintf(format, args...))
}

func escapeName(s string) string {
	return s // TODO
}

func (tx *Tx) table(h tuple.Header) tuple.Table {
	return &Table{tx: tx, h: h}
}
func (tx *Tx) asTable(t *gocql.TableMetadata) (tuple.Table, error) {
	h := tuple.Header{Name: t.Name}
	for _, name := range t.OrderedColumns {
		c := t.Columns[name]
		switch c.Kind {
		case gocql.ColumnPartitionKey:
			h.Key = append(h.Key, tuple.KeyField{
				Name: c.Name,
				Type: revTypeMap[c.Validator].(tuple.KeyType),
			})
		case gocql.ColumnRegular:
			h.Data = append(h.Data, tuple.Field{
				Name: c.Name,
				Type: revTypeMap[c.Validator],
			})
		default:
			return nil, fmt.Errorf("unknown column type: %v", c.Kind)
		}
	}
	return tx.table(h), nil
}
func (tx *Tx) Table(ctx context.Context, name string) (tuple.Table, error) {
	m, err := tx.sess.KeyspaceMetadata(tx.ns)
	if err != nil {
		return nil, err
	}
	t, ok := m.Tables[name]
	if !ok {
		// FIXME: some schema consistency issues, or driver cache
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		m, err = tx.sess.KeyspaceMetadata(tx.ns)
		if err != nil {
			return nil, err
		}
		t, ok = m.Tables[name]
	}
	if !ok {
		return nil, tuple.ErrTableNotFound
	}
	return tx.asTable(t)
}

func (tx *Tx) ListTables(ctx context.Context) ([]tuple.Table, error) {
	m, err := tx.sess.KeyspaceMetadata(tx.ns)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range m.Tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	var out []tuple.Table
	for _, name := range names {
		t := m.Tables[name]
		tbl, err := tx.asTable(t)
		if err != nil {
			return out, err
		}
		out = append(out, tbl)
	}
	return out, nil
}

func (tx *Tx) CreateTable(ctx context.Context, table tuple.Header) (tuple.Table, error) {
	var fields []string
	var pk []string
	for _, f := range table.Key {
		typ, ok := typeMap[f.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported type: %T", f.Type)
		}
		name := escapeName(f.Name)
		fields = append(fields, fmt.Sprintf("%s %s", name, typ))
		pk = append(pk, name)
	}
	for _, f := range table.Data {
		typ, ok := typeMap[f.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported type: %T", f.Type)
		}
		name := escapeName(f.Name)
		fields = append(fields, fmt.Sprintf("%s %s", name, typ))
	}
	fields = append(fields, fmt.Sprintf(`PRIMARY KEY ( %s )`, strings.Join(pk, ", ")))
	qu := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", escapeName(table.Name), strings.Join(fields, ",\n"))
	err := tx.query(ctx, qu).Exec()
	if err != nil {
		// TODO: ErrExists
		return nil, err
	}
	return tx.table(table.Clone()), nil
}

type Table struct {
	tx *Tx
	h  tuple.Header
}

func (tbl *Table) Delete(ctx context.Context) error {
	return tbl.tx.queryf(ctx, `DROP TABLE %s`, tbl.h.Name).Exec()
}

func (tbl *Table) Clear(ctx context.Context) error {
	return tbl.tx.queryf(ctx, `TRUNCATE TABLE %s`, tbl.h.Name).Exec()
}

func (tbl *Table) GetTuple(ctx context.Context, key tuple.Key) (tuple.Data, error) {
	if err := tbl.h.ValidateKey(key, false); err != nil {
		return nil, err
	}
	var (
		fields []string
		out    tuple.Data
		dest   []interface{}
	)
	for _, f := range tbl.h.Data {
		v := f.Type.New()
		fields = append(fields, escapeName(f.Name))
		out = append(out, v)
		dest = append(dest, v)
	}
	var (
		where []string
		vals  []interface{}
	)
	for i, f := range tbl.h.Key {
		where = append(where, fmt.Sprintf("(%s = ?)", escapeName(f.Name)))
		vals = append(vals, key[i].Native())
	}
	qu := fmt.Sprintf(`SELECT %s FROM %s WHERE %s`,
		strings.Join(fields, ", "),
		escapeName(tbl.h.Name),
		strings.Join(where, " AND "),
	)
	err := tbl.tx.query(ctx, qu, vals...).Scan(dest...)
	if err == gocql.ErrNotFound {
		return nil, tuple.ErrNotFound
	}
	return out, err
}

func (tbl *Table) GetTupleBatch(ctx context.Context, keys []tuple.Key) ([]tuple.Data, error) {
	panic("implement me")
}

func (tbl *Table) InsertTuple(ctx context.Context, t tuple.Tuple) (tuple.Key, error) {
	if err := tbl.h.ValidateKey(t.Key, true); err != nil {
		return nil, err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return nil, err
	}
	if tbl.h.Key[0].Auto {
		// FIXME: auto fields
		return nil, fmt.Errorf("auto fields are not yet supported")
	}
	var fields []string
	var places []string
	for _, f := range tbl.h.Key {
		fields = append(fields, escapeName(f.Name))
		places = append(places, "?")
	}
	for _, f := range tbl.h.Data {
		fields = append(fields, escapeName(f.Name))
		places = append(places, "?")
	}
	var vals []interface{}
	for _, v := range t.Key {
		vals = append(vals, v.Native())
	}
	for _, v := range t.Data {
		vals = append(vals, v.Native())
	}
	qu := fmt.Sprintf(`INSERT INTO %s ( %s ) VALUES ( %s )`,
		escapeName(tbl.h.Name), strings.Join(fields, ", "), strings.Join(places, ", "))
	err := tbl.tx.query(ctx, qu, vals...).Exec()
	if err != nil {
		return nil, err
	}
	return t.Key, nil
}

func (tbl *Table) UpdateTuple(ctx context.Context, t tuple.Tuple, opt *tuple.UpdateOpt) error {
	panic("implement me")
}

func (tbl *Table) DeleteTuple(ctx context.Context, key tuple.Key) error {
	panic("implement me")
}

func (tbl *Table) Scan(pref tuple.Key) tuple.Iterator {
	return &Iterator{tbl: tbl, pref: pref}
}

type Iterator struct {
	tbl  *Table
	pref tuple.Key

	key tuple.Key
	row tuple.Data
	it  *gocql.Iter
	err error
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.it != nil {
		return it.it.Close()
	}
	return nil
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.it == nil {
		if err := it.tbl.h.ValidatePref(it.pref); err != nil {
			it.err = err
			return false
		}
		var (
			fields []string
			where  []string
			vals   []interface{}
		)
		for i, f := range it.tbl.h.Key {
			fields = append(fields, escapeName(f.Name))
			if i < len(it.pref) {
				where = append(where, fmt.Sprintf("(%s = ?)", escapeName(f.Name)))
				vals = append(vals, it.pref[i].Native())
			}
		}
		for _, f := range it.tbl.h.Data {
			fields = append(fields, escapeName(f.Name))
		}
		qu := fmt.Sprintf(`SELECT %s FROM %s`,
			strings.Join(fields, ", "),
			escapeName(it.tbl.h.Name),
		)
		if len(where) != 0 {
			qu += fmt.Sprintf(" WHERE %s", strings.Join(where, " AND "))
		}
		it.it = it.tbl.tx.query(context.TODO(), qu, vals...).Iter()
	}
	it.key = nil
	it.row = nil
	var dest []interface{}
	for _, f := range it.tbl.h.Key {
		v := f.Type.NewSortable()
		it.key = append(it.key, v)
		dest = append(dest, v)
	}
	for _, f := range it.tbl.h.Data {
		v := f.Type.New()
		it.row = append(it.row, v)
		dest = append(dest, v)
	}
	if it.it.Scan(dest...) {
		return true
	}
	it.err = it.it.Close()
	return false
}

func (it *Iterator) Key() tuple.Key {
	return it.key
}

func (it *Iterator) Data() tuple.Data {
	return it.row
}

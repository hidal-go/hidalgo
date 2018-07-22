package sqltuple

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/values"
)

const (
	debug = false
)

var (
	ErrTableNotFound = tuple.ErrTableNotFound
)

func OpenSQL(name, addr, db string) (*sql.DB, error) {
	r := ByName(name)
	if r == nil {
		return nil, fmt.Errorf("not registered: %q", name)
	}
	dsn, err := r.DSN(addr, db)
	if err != nil {
		return nil, err
	}
	return sql.Open(r.Driver, dsn)
}

func Open(name, addr, db string) (tuple.Store, error) {
	conn, err := OpenSQL(name, addr, db)
	if err != nil {
		return nil, err
	}
	return New(conn, db, ByName(name).Dialect), nil
}

func New(db *sql.DB, dbname string, dia Dialect) tuple.Store {
	dia.SetDefaults()
	return &sqlStore{db: db, dbName: dbname, dia: dia}
}

type sqlStore struct {
	db     *sql.DB
	dbName string
	dia    Dialect
}

func (s *sqlStore) Close() error {
	return s.db.Close()
}

func (s *sqlStore) Tx(rw bool) (tuple.Tx, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTx{dia: &s.dia, tx: tx, dbName: s.dbName, rw: rw}, nil
}

type sqlTx struct {
	dia    *Dialect
	tx     *sql.Tx
	dbName string
	rw     bool
	mu     sync.RWMutex
	tables map[string]*sqlTable
}

func (tx *sqlTx) curSchema() string {
	if s := tx.dia.DefaultSchema; s != "" {
		return s
	}
	return tx.dbName
}

func (tx *sqlTx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *sqlTx) Close() error {
	return tx.tx.Rollback()
}

func (tx *sqlTx) convError(err error) error {
	if tx.dia.Errors != nil {
		err = tx.dia.Errors(err)
	}
	switch err {
	case ErrTableNotFound:
		return tuple.ErrTableNotFound
	}
	return err
}

func (tx *sqlTx) query(ctx context.Context, qu string, args ...interface{}) (*sql.Rows, error) {
	if debug {
		log.Println(append([]interface{}{qu}, args...)...)
	}
	rows, err := tx.tx.QueryContext(ctx, qu, args...)
	if err != nil {
		err = tx.convError(err)
	}
	return rows, err
}

func (tx *sqlTx) queryb(ctx context.Context, b *Builder) (*sql.Rows, error) {
	qu, args := b.String(), b.Args()
	return tx.query(ctx, qu, args...)
}

func (tx *sqlTx) queryRow(ctx context.Context, qu string, args ...interface{}) *sql.Row {
	if debug {
		log.Println(append([]interface{}{qu}, args...)...)
	}
	return tx.tx.QueryRowContext(ctx, qu, args...)
}

func (tx *sqlTx) exec(ctx context.Context, qu string, args ...interface{}) error {
	if debug {
		log.Println(append([]interface{}{qu}, args...)...)
	}
	// TODO: prepare automatically
	_, err := tx.tx.ExecContext(ctx, qu, args...)
	err = tx.convError(err)
	return err
}

func (tx *sqlTx) execb(ctx context.Context, b *Builder) error {
	qu, args := b.String(), b.Args()
	return tx.exec(ctx, qu, args...)
}

func (tx *sqlTx) execStmt(ctx context.Context, st *sql.Stmt, args ...interface{}) error {
	if debug {
		log.Println(append([]interface{}{"STMT"}, args...)...)
	}
	_, err := st.ExecContext(ctx, args...)
	if err != nil {
		err = tx.convError(err)
	}
	return err
}

func (tx *sqlTx) cacheTable(tbl *sqlTable) {
	if debug {
		log.Printf("%q, %#v, %#v", tbl.h.Name, tbl.h.Key, tbl.h.Data)
	}
	if tx.tables == nil {
		tx.tables = make(map[string]*sqlTable)
	}
	tx.mu.Lock()
	tx.tables[tbl.h.Name] = tbl
	tx.mu.Unlock()
}

func (tx *sqlTx) table(name string) *sqlTable {
	tx.mu.RLock()
	tbl := tx.tables[name]
	tx.mu.RUnlock()
	return tbl
}

func (tx *sqlTx) Table(ctx context.Context, name string) (tuple.Table, error) {
	if tbl := tx.table(name); tbl != nil {
		return tbl, nil
	}
	tbl := &sqlTable{tx: tx, h: tuple.Header{Name: name}}
	rows, err := tx.query(ctx, tx.dia.ListColumns,
		tx.curSchema(),
		name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type column struct {
		Name    string
		Type    string
		Null    string         // YES/NO
		Key     sql.NullString // PRI*
		Comment sql.NullString
	}
	var cols []column
	for rows.Next() {
		var col column
		if err := rows.Scan(
			&col.Name, &col.Type, &col.Null, &col.Key, &col.Comment,
		); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	} else if len(cols) == 0 {
		return nil, ErrTableNotFound
	}
	for _, c := range cols {
		typ, err := tbl.nativeType(c.Type, c.Comment.String)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(c.Key.String, "PRI") {
			kt, ok := typ.(values.SortableType)
			if !ok {
				return nil, fmt.Errorf("non-sortable key type: %T", typ)
			}
			tbl.h.Key = append(tbl.h.Key, tuple.KeyField{
				Name: c.Name,
				Type: kt,
			})
		} else {
			tbl.h.Data = append(tbl.h.Data, tuple.Field{
				Name: c.Name,
				Type: typ,
			})
		}
	}
	tx.cacheTable(tbl)
	return tbl, nil
}

func (tx *sqlTx) ListTables(ctx context.Context) ([]tuple.Table, error) {
	rows, err := tx.query(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = `+tx.dia.Placeholder(0),
		tx.curSchema(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []tuple.Table
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return tables, err
		}
		// TODO: invalidate table cache before listing?
		tbl, err := tx.Table(ctx, name)
		if err != nil {
			return tables, err
		}
		tables = append(tables, tbl)
	}
	return tables, nil
}

func (tx *sqlTx) CreateTable(ctx context.Context, table tuple.Header) (tuple.Table, error) {
	if !tx.rw {
		return nil, tuple.ErrReadOnly
	} else if err := table.Validate(); err != nil {
		return nil, err
	}
	tbl := &sqlTable{tx: tx, h: table}
	b := tbl.sql()
	b.Write("CREATE TABLE ")
	b.Idents(tbl.h.Name)
	b.Write(" (")
	for i, f := range table.Key {
		if i > 0 {
			b.Write(",")
		}
		b.Write("\n\t")
		b.Idents(f.Name)
		b.Write(" ")
		b.Write(tbl.sqlColumnDef(f.Type, true))
	}
	for _, f := range table.Data {
		b.Write(",\n\t")
		b.Idents(f.Name)
		b.Write(" ")
		b.Write(tbl.sqlColumnDef(f.Type, false))
	}
	if len(tbl.h.Key) != 0 {
		b.Write(",\n\t")
		b.Write("PRIMARY KEY (")
		b.Idents(tbl.keyNames()...)
		b.Write(")")
	}
	b.Write("\n);")
	err := tx.execb(ctx, b)
	if err != nil {
		return nil, err
	}
	b.Reset()
	if dia := tbl.tx.dia; dia.ColumnCommentInline == nil && dia.ColumnCommentSet != nil {
		setComment := func(col string, t values.Type) error {
			c := dia.sqlColumnComment(t)
			if c == "" {
				return nil
			}
			b.Reset()
			dia.ColumnCommentSet(b, tbl.h.Name, col, c)
			return tx.execb(ctx, b)
		}
		for _, f := range table.Key {
			if err := setComment(f.Name, f.Type); err != nil {
				return nil, err
			}
		}
		for _, f := range table.Data {
			if err := setComment(f.Name, f.Type); err != nil {
				return nil, err
			}
		}
	}
	tx.cacheTable(tbl)
	return tbl, nil
}

type sqlTable struct {
	tx *sqlTx
	h  tuple.Header
}

func (tbl *sqlTable) sqlType(t values.Type, key bool) string {
	return tbl.tx.dia.sqlType(t, key)
}
func (tbl *sqlTable) sqlColumnDef(t values.Type, key bool) string {
	return tbl.sqlType(t, key) + tbl.tx.dia.sqlColumnCommentInline(t)
}
func (tbl *sqlTable) nativeType(typ, comment string) (values.Type, error) {
	return tbl.tx.dia.nativeType(typ, comment)
}

func (tbl *sqlTable) Drop(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	b := tbl.sql()
	b.Write(`DROP TABLE `)
	b.Idents(tbl.h.Name)
	return tbl.tx.execb(ctx, b)
}

func (tbl *sqlTable) Clear(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	b := tbl.sql()
	b.Write(`TRUNCATE TABLE `)
	b.Idents(tbl.h.Name)
	return tbl.tx.execb(ctx, b)
}
func (tbl *sqlTable) convValue(v values.Value) interface{} {
	if v == nil {
		return nil
	}
	return v.Native()
}
func (tbl *sqlTable) appendKey(dst []interface{}, key tuple.Key) []interface{} {
	for _, k := range key {
		dst = append(dst, tbl.convValue(k))
	}
	return dst
}
func (tbl *sqlTable) appendData(dst []interface{}, data tuple.Data) []interface{} {
	for _, d := range data {
		dst = append(dst, tbl.convValue(d))
	}
	return dst
}
func (tbl *sqlTable) appendTuple(dst []interface{}, t tuple.Tuple) []interface{} {
	dst = tbl.appendKey(dst, t.Key)
	dst = tbl.appendData(dst, t.Data)
	return dst
}
func (tbl *sqlTable) names() []string {
	names := make([]string, 0, len(tbl.h.Key)+len(tbl.h.Data))
	for _, f := range tbl.h.Key {
		names = append(names, f.Name)
	}
	for _, f := range tbl.h.Data {
		names = append(names, f.Name)
	}
	return names
}
func (tbl *sqlTable) keyNames() []string {
	names := make([]string, 0, len(tbl.h.Key))
	for _, f := range tbl.h.Key {
		names = append(names, f.Name)
	}
	return names
}
func (tbl *sqlTable) payloadNames() []string {
	names := make([]string, 0, len(tbl.h.Data))
	for _, f := range tbl.h.Data {
		names = append(names, f.Name)
	}
	return names
}

type scanner interface {
	Scan(dst ...interface{}) error
}

func (tbl *sqlTable) scanTuple(row scanner) (tuple.Tuple, error) {
	key := make([]values.SortableDest, 0, len(tbl.h.Key))
	data := make([]values.ValueDest, 0, len(tbl.h.Data))
	in := make([]interface{}, 0, cap(key)+cap(data))

	for _, f := range tbl.h.Key {
		v := f.Type.NewSortable()
		key = append(key, v)
		in = append(in, v.NativePtr())
	}
	for _, f := range tbl.h.Data {
		v := f.Type.New()
		data = append(data, v)
		in = append(in, v.NativePtr())
	}

	if err := row.Scan(in...); err != nil {
		return tuple.Tuple{}, err
	}

	t := tuple.Tuple{
		Key:  make(tuple.Key, 0, len(key)),
		Data: make(tuple.Data, 0, len(data)),
	}

	for _, k := range key {
		v := k.Sortable()
		t.Key = append(t.Key, v)
	}
	for _, d := range data {
		v := d.Value()
		t.Data = append(t.Data, v)
	}
	return t, nil
}
func (tbl *sqlTable) scanPayload(row scanner) (tuple.Data, error) {
	dest := make([]values.ValueDest, 0, len(tbl.h.Data))
	in := make([]interface{}, 0, cap(dest))

	for _, f := range tbl.h.Data {
		v := f.Type.New()
		dest = append(dest, v)
		in = append(in, v.NativePtr())
	}

	if err := row.Scan(in...); err != nil {
		return nil, err
	}

	data := make(tuple.Data, 0, len(dest))
	for _, d := range dest {
		v := d.Value()
		data = append(data, v)
	}
	return data, nil
}
func (tbl *sqlTable) GetTuple(ctx context.Context, key tuple.Key) (tuple.Data, error) {
	if err := tbl.h.ValidateKey(key, false); err != nil {
		return nil, err
	}
	b := tbl.sql()
	b.Write("SELECT ")
	b.Idents(tbl.payloadNames()...)
	b.Write(" FROM ")
	b.Idents(tbl.h.Name)
	b.Write(" WHERE ")
	b.EqPlaceAnd(tbl.keyNames(), tbl.appendKey(nil, key))
	b.Write(" LIMIT 1")
	row := tbl.tx.queryRow(ctx, b.String(), b.Args()...)
	data, err := tbl.scanPayload(row)
	if err == sql.ErrNoRows {
		return nil, tuple.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (tbl *sqlTable) GetTupleBatch(ctx context.Context, keys []tuple.Key) ([]tuple.Data, error) {
	out := make([]tuple.Data, 0, len(keys))
	// TODO: batch select
	for _, k := range keys {
		d, err := tbl.GetTuple(ctx, k)
		if err != nil {
			return out, err
		}
		out = append(out, d)
	}
	return out, nil
}

func (tbl *sqlTable) InsertTuple(ctx context.Context, t tuple.Tuple) (tuple.Key, error) {
	if err := tbl.h.ValidateKey(t.Key, true); err != nil {
		return nil, err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return nil, err
	}
	if tbl.h.Key[0].Auto {
		// FIXME: auto fields
		return nil, fmt.Errorf("auto fields are not yet supported")
	}
	b := tbl.sql()
	b.Write("INSERT INTO ")
	b.Idents(tbl.h.Name)
	b.Write("(")
	b.Idents(tbl.names()...)
	b.Write(") VALUES (")
	b.Place(tbl.appendTuple(nil, t)...)
	b.Write(")")
	err := tbl.tx.execb(ctx, b)
	if err != nil {
		return nil, err
	}
	return t.Key, nil
}

func (tbl *sqlTable) sql() *Builder {
	return tbl.tx.dia.NewBuilder()
}
func (tbl *sqlTable) UpdateTuple(ctx context.Context, t tuple.Tuple, opt *tuple.UpdateOpt) error {
	if err := tbl.h.ValidateKey(t.Key, false); err != nil {
		return err
	} else if err = tbl.h.ValidateData(t.Data); err != nil {
		return err
	}
	if opt == nil {
		opt = &tuple.UpdateOpt{}
	}
	if !opt.Upsert {
		b := tbl.sql()
		b.Write(`UPDATE `)
		b.Idents(tbl.h.Name)
		b.Write(` SET `)
		b.EqPlace(tbl.payloadNames(), tbl.appendData(nil, t.Data))
		b.Write(` WHERE `)
		b.EqPlaceAnd(tbl.keyNames(), tbl.appendKey(nil, t.Key))
		return tbl.tx.execb(ctx, b)
	}
	dia := tbl.tx.dia
	if dia.OnConflict {
		b := tbl.sql()
		b.Write(`INSERT INTO `)
		b.Idents(tbl.h.Name)
		b.Write(`(`)
		b.Idents(tbl.names()...)
		b.Write(`) VALUES (`)
		b.Place(tbl.appendTuple(nil, t)...)
		b.Write(`) ON CONFLICT ON CONSTRAINT `)
		b.Idents(tbl.h.Name + "_pkey") // TODO: should be in the dialect
		b.Write(` DO UPDATE SET `)
		b.EqPlace(tbl.payloadNames(), tbl.appendData(nil, t.Data))
		return tbl.tx.execb(ctx, b)
	}
	if dia.ReplaceStmt {
		b := tbl.sql()
		b.Write("REPLACE INTO ")
		b.Idents(tbl.h.Name)
		b.Write("(")
		b.Idents(tbl.names()...)
		b.Write(") VALUES (")
		b.Place(tbl.appendTuple(nil, t)...)
		b.Write(")")
		return tbl.tx.execb(ctx, b)
	}
	return fmt.Errorf("upsert is not supported")
}

func (tbl *sqlTable) asWhere(f *tuple.Filter) (*tuple.Filter, func(*Builder)) {
	// FIXME: optimize filters
	return f, nil
}
func (tbl *sqlTable) DeleteTuples(ctx context.Context, f *tuple.Filter) error {
	f, where := tbl.asWhere(f)
	if f.IsAny() {
		// no additional filters - delete directly
		b := tbl.sql()
		b.Write(`DELETE FROM `)
		b.Idents(tbl.h.Name)
		if where != nil {
			b.Write(" WHERE ")
			where(b)
		}
		return tbl.tx.execb(ctx, b)
	}
	// some filter were optimized, but some still remain
	// fallback to iterate + delete

	// TODO: select key only
	it := tbl.scanWhere(tuple.SortNone, where, f)
	defer it.Close()

	var allKeys []tuple.Key
	if tbl.tx.dia.NoIteratorsWhenMutating {
		// Some databases cannot handle multiple requests over the single connection,
		// so we can't keep an iterator open and send delete requests. We are forced
		// to pull all results to memory first.
		// TODO: run select with a limit, delete and rerun it again
		for it.Next(ctx) {
			allKeys = append(allKeys, it.Key())
		}
		if err := it.Err(); err != nil {
			return err
		}
	}

	deleteOne := func(k tuple.Key) error {
		b := tbl.sql()
		b.Write(`DELETE FROM `)
		b.Idents(tbl.h.Name)
		b.Write(" WHERE ")
		b.EqPlaceAnd(tbl.keyNames(), tbl.appendKey(nil, k))
		return tbl.tx.execb(ctx, b)
	}

	if tbl.tx.dia.NoIteratorsWhenMutating {
		for _, key := range allKeys {
			err := deleteOne(key)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for it.Next(ctx) {
		key := it.Key()
		err := deleteOne(key)
		if err != nil {
			return err
		}
	}
	return it.Err()
}

func (tbl *sqlTable) scan(open rowsFunc, f *tuple.Filter) tuple.Iterator {
	return &sqlIterator{tbl: tbl, open: open, f: f}
}

func (tbl *sqlTable) scanWhere(sorting tuple.Sorting, where func(*Builder), f *tuple.Filter) tuple.Iterator {
	return tbl.scan(func(ctx context.Context) (*sql.Rows, error) {
		b := tbl.sql()
		b.Write(`SELECT `)
		b.Idents(tbl.names()...)
		b.Write(` FROM `)
		b.Idents(tbl.h.Name)
		if where != nil {
			b.Write(" WHERE ")
			where(b)
		}
		dir := ""
		switch sorting {
		case tuple.SortAsc:
			dir = "ASC"
		case tuple.SortDesc:
			dir = "DESC"
		}
		if dir != "" {
			b.Write(" ORDER BY ")
			b.Idents(tbl.keyNames()...)
			b.Write(" " + dir)
		}
		return tbl.tx.queryb(ctx, b)
	}, f)
}

func (tbl *sqlTable) Scan(sorting tuple.Sorting, f *tuple.Filter) tuple.Iterator {
	f, where := tbl.asWhere(f)
	return tbl.scanWhere(sorting, where, f)
}

type rowsFunc func(ctx context.Context) (*sql.Rows, error)

type sqlIterator struct {
	tbl *sqlTable

	rows *sql.Rows
	open rowsFunc
	f    *tuple.Filter

	t   *tuple.Tuple
	err error
}

func (it *sqlIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.rows == nil {
		// TODO: this context might be captured by the iterator
		rows, err := it.open(ctx)
		if err != nil {
			it.err = err
			return false
		}
		it.rows = rows
	}
	return tuple.FilterIterator(it, it.f, func() bool {
		it.t = nil
		return it.rows.Next()
	})
}

func (it *sqlIterator) Err() error {
	if it.err != nil || it.rows == nil {
		return it.err
	}
	return it.rows.Err()
}

func (it *sqlIterator) Close() error {
	if it.rows == nil {
		return nil
	}
	return it.rows.Close()
}

func (it *sqlIterator) scan() {
	if it.t != nil || it.rows == nil {
		return
	}
	t, err := it.tbl.scanTuple(it.rows)
	if err != nil {
		// TODO: user might skip this error
		it.err = err
		return
	}
	it.t = &t
}
func (it *sqlIterator) Key() tuple.Key {
	it.scan()
	if it.t == nil {
		return nil
	}
	return it.t.Key
}

func (it *sqlIterator) Data() tuple.Data {
	it.scan()
	if it.t == nil {
		return nil
	}
	return it.t.Data
}

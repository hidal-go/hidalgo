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
	debug         = false
	deletePullAll = true
)

var (
	ErrTableNotFound = tuple.ErrTableNotFound
)

type ErrorFunc func(err error) error

type Dialect struct {
	Errors ErrorFunc
}

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
	return New(conn, ByName(name).Dialect), nil
}

func New(db *sql.DB, dia Dialect) tuple.Store {
	return &sqlStore{db: db, dia: dia}
}

type sqlStore struct {
	db  *sql.DB
	dia Dialect
}

func (s *sqlStore) Close() error {
	return s.db.Close()
}

func (s *sqlStore) Tx(rw bool) (tuple.Tx, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTx{dia: &s.dia, tx: tx, rw: rw}, nil
}

type sqlTx struct {
	dia    *Dialect
	tx     *sql.Tx
	rw     bool
	mu     sync.RWMutex
	tables map[string]*sqlTable
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

func (tx *sqlTx) prepare(ctx context.Context, qu string) (*sql.Stmt, error) {
	if debug {
		log.Println(qu)
	}
	stmt, err := tx.tx.PrepareContext(ctx, qu)
	if err != nil {
		err = tx.convError(err)
	}
	return stmt, err
}

func (tx *sqlTx) query(ctx context.Context, qu string, args ...interface{}) (*sql.Rows, error) {
	if debug {
		log.Println(qu, args)
	}
	rows, err := tx.tx.QueryContext(ctx, qu, args...)
	if err != nil {
		err = tx.convError(err)
	}
	return rows, err
}

func (tx *sqlTx) queryRow(ctx context.Context, qu string, args ...interface{}) *sql.Row {
	if debug {
		log.Println(qu, args)
	}
	return tx.tx.QueryRowContext(ctx, qu, args...)
}

func (tx *sqlTx) exec(ctx context.Context, qu string, args ...interface{}) error {
	if debug {
		log.Println(qu, args)
	}
	_, err := tx.tx.ExecContext(ctx, qu, args...)
	err = tx.convError(err)
	return err
}

func (tx *sqlTx) execStmt(ctx context.Context, st *sql.Stmt, args ...interface{}) error {
	if debug {
		log.Println("STMT", args)
	}
	_, err := st.ExecContext(ctx, args...)
	if err != nil {
		err = tx.convError(err)
	}
	return err
}

func (tx *sqlTx) cacheTable(tbl *sqlTable) {
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
	rows, err := tx.query(ctx, `SHOW FULL COLUMNS FROM `+tbl.name())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type column struct {
		Name       string
		Type       string
		Collation  *sql.NullString
		Null       string // YES/NO
		Key        string // PRI
		Default    *sql.NullString
		Extra      string
		Privileges string
		Comment    string
	}
	var cols []column
	for rows.Next() {
		var col column
		if err := rows.Scan(
			&col.Name, &col.Type, &col.Collation, &col.Null, &col.Key,
			&col.Default, &col.Extra, &col.Privileges, &col.Comment,
		); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	for _, c := range cols {
		typ, err := tbl.typeFromMeta(c.Type, c.Comment)
		if err != nil {
			return nil, err
		}
		if c.Key == "PRI" {
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
	rows, err := tx.query(ctx, `SHOW TABLES`)
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
	qu := `CREATE TABLE ` + tbl.name() + ` (`
	for i, f := range table.Key {
		if i > 0 {
			qu += ","
		}
		qu += "\n" + tbl.quoteName(f.Name) + " " + tbl.sqlColumnDef(f.Type, true)
	}
	for _, f := range table.Data {
		qu += ",\n" + tbl.quoteName(f.Name) + " " + tbl.sqlColumnDef(f.Type, false)
	}
	if len(tbl.h.Key) != 0 {
		qu += ",\nPRIMARY KEY (" + tbl.keyNames() + ")"
	}
	qu += "\n);"
	err := tx.exec(ctx, qu)
	if err != nil {
		return nil, err
	}
	tx.cacheTable(tbl)
	return tbl, nil
}

type sqlTable struct {
	tx *sqlTx
	h  tuple.Header
}

func (tbl *sqlTable) sqlType(t values.Type, key bool) string {
	var tp string
	switch t.(type) {
	case values.StringType:
		if key {
			// TODO: only MySQL
			// TODO: pick size based on the number of columns (max 3k)
			tp = "VARCHAR(256)"
		} else {
			tp = "TEXT"
		}
		// TODO: set it on the table/database
		tp += " CHARACTER SET utf8 COLLATE utf8_unicode_ci"
	case values.BytesType:
		if key {
			// TODO: only MySQL
			// TODO: pick size based on the number of columns (max 3k)
			tp = "VARBINARY(256)"
		} else {
			// TODO: MySQL: BLOB or VARBINARY
			// TODO: PostgreSQL: BYTEA
			tp = "BLOB"
		}
	case values.IntType:
		tp = "BIGINT"
	case values.UIntType:
		tp = "BIGINT UNSIGNED"
	case values.FloatType:
		tp = "DOUBLE PRECISION"
	case values.BoolType:
		tp = "BOOLEAN"
	case values.TimeType:
		tp = "DATETIME(6)" // TODO: PostgreSQL: TIMESTAMP
	default:
		panic(fmt.Errorf("unsupported type: %T", t))
	}
	if key {
		tp += " NOT NULL"
	} else {
		tp += " NULL"
	}
	return tp
}
func (tbl *sqlTable) sqlColumnMeta(t values.Type) string {
	switch t.(type) {
	case values.BoolType:
		return " COMMENT " + tbl.quoteString("Bool")
	}
	return ""
}
func (tbl *sqlTable) sqlColumnDef(t values.Type, key bool) string {
	return tbl.sqlType(t, key) + tbl.sqlColumnMeta(t)
}
func (tbl *sqlTable) typeFromMeta(typ, comment string) (values.Type, error) {
	typ = strings.ToLower(typ)
	switch typ {
	case "text":
		return values.StringType{}, nil
	case "blob", "bytea":
		return values.BytesType{}, nil
	case "double", "double precision":
		return values.FloatType{}, nil
	case "boolean":
		return values.BoolType{}, nil
	case "tinyint(1)":
		if comment == "Bool" { // TODO: or if it's MySQL
			return values.BoolType{}, nil
		}
	case "timestamp", "datetime", "date", "time":
		return values.TimeType{}, nil
	}
	pref := func(p string) bool {
		return strings.HasPrefix(typ, p)
	}
	switch {
	case pref("blob"), pref("varbinary"), pref("binary"):
		return values.BytesType{}, nil
	case pref("text"), pref("varchar"), pref("char"):
		return values.StringType{}, nil
	case pref("bigint"), pref("int"), pref("mediumint"), pref("smallint"), pref("tinyint"):
		if strings.HasSuffix(typ, "unsigned") {
			return values.UIntType{}, nil
		}
		return values.IntType{}, nil
	case pref("timestamp"), pref("datetime"), pref("date"), pref("time"):
		return values.TimeType{}, nil
	}
	return nil, fmt.Errorf("unsupported column type: %q", typ)
}

func (tbl *sqlTable) Drop(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	return tbl.tx.exec(ctx, `DROP TABLE `+tbl.name())
}

func (tbl *sqlTable) Clear(ctx context.Context) error {
	if !tbl.tx.rw {
		return tuple.ErrReadOnly
	}
	return tbl.tx.exec(ctx, `TRUNCATE TABLE `+tbl.name())
}
func (tbl *sqlTable) name() string {
	return tbl.quoteName(tbl.h.Name)
}
func (tbl *sqlTable) quoteName(s string) string {
	// TODO: PostgreSQL
	return "`" + s + "`"
}
func (tbl *sqlTable) quoteString(s string) string {
	// TODO: PostgreSQL
	return "'" + s + "'"
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
func (tbl *sqlTable) whereKey() string {
	where := make([]string, 0, len(tbl.h.Key))
	for _, f := range tbl.h.Key {
		where = append(where, `(`+tbl.quoteName(f.Name)+` = ?)`)
	}
	return strings.Join(where, " AND ")
}
func (tbl *sqlTable) names() string {
	names := make([]string, 0, len(tbl.h.Key)+len(tbl.h.Data))
	for _, f := range tbl.h.Key {
		names = append(names, tbl.quoteName(f.Name))
	}
	for _, f := range tbl.h.Data {
		names = append(names, tbl.quoteName(f.Name))
	}
	return strings.Join(names, ", ")
}
func (tbl *sqlTable) placeholders() string {
	ph := make([]string, len(tbl.h.Key)+len(tbl.h.Data))
	for i := range ph {
		ph[i] = "?" // TODO: PostgreSQL
	}
	return strings.Join(ph, ", ")
}
func (tbl *sqlTable) keyNames() string {
	names := make([]string, 0, len(tbl.h.Key))
	for _, f := range tbl.h.Key {
		names = append(names, tbl.quoteName(f.Name))
	}
	return strings.Join(names, ", ")
}
func (tbl *sqlTable) payloadNames() string {
	names := make([]string, 0, len(tbl.h.Data))
	for _, f := range tbl.h.Data {
		names = append(names, tbl.quoteName(f.Name))
	}
	return strings.Join(names, ", ")
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
	qu := `SELECT ` + tbl.payloadNames() + ` FROM ` + tbl.name() + ` WHERE ` + tbl.whereKey() + ` LIMIT 1;`
	args := tbl.appendKey(nil, key)
	row := tbl.tx.queryRow(ctx, qu, args...)
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

func (tbl *sqlTable) insertTuple(ctx context.Context, op string, stmt *sql.Stmt, t tuple.Tuple) (tuple.Key, *sql.Stmt, error) {
	if stmt == nil {
		qu := op + ` INTO ` + tbl.name() + `(` + tbl.names() + `) VALUES (` + tbl.placeholders() + `)`
		var err error
		stmt, err = tbl.tx.prepare(ctx, qu)
		if err != nil {
			return nil, nil, err
		}
	}
	rec := make([]interface{}, 0, len(tbl.h.Key)+len(tbl.h.Data))
	rec = tbl.appendKey(rec, t.Key)
	rec = tbl.appendData(rec, t.Data)
	err := tbl.tx.execStmt(ctx, stmt, rec...)
	if err != nil {
		return nil, stmt, err
	}
	return t.Key, stmt, nil
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
	key, stmt, err := tbl.insertTuple(ctx, "INSERT", nil, t)
	if stmt != nil {
		stmt.Close()
	}
	return key, err
}

func (tbl *sqlTable) setData() string {
	set := make([]string, 0, len(tbl.h.Data))
	for _, f := range tbl.h.Data {
		set = append(set, tbl.quoteName(f.Name)+` = ?`)
	}
	return strings.Join(set, ", ")
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
		var args []interface{}
		args = tbl.appendData(args, t.Data)
		args = tbl.appendKey(args, t.Key)
		qu := `UPDATE ` + tbl.name() + ` SET ` + tbl.setData() + ` WHERE ` + tbl.whereKey()
		return tbl.tx.exec(ctx, qu, args...)
	}
	_, stmt, err := tbl.insertTuple(ctx, "REPLACE", nil, t)
	if stmt != nil {
		stmt.Close()
	}
	return err
}

func (tbl *sqlTable) asWhere(f *tuple.Filter) (string, []interface{}, *tuple.Filter) {
	// FIXME: optimize filters
	return "", nil, f
}
func (tbl *sqlTable) DeleteTuples(ctx context.Context, f *tuple.Filter) error {
	where, args, f := tbl.asWhere(f)
	qu := `DELETE FROM ` + tbl.name()
	if f.IsAny() {
		// no additional filters - delete directly
		qu += where
		return tbl.tx.exec(ctx, qu, args...)
	}
	// some filter were optimized, but some still remain
	// fallback to iterate + delete

	// TODO: select key only
	it := tbl.scanWhere(where, args, f)
	defer it.Close()

	qu += " WHERE " + tbl.whereKey()

	var allKeys []tuple.Key
	if deletePullAll {
		// Some databases (MySQL) cannot handle multiple requests over
		// the single connection, so we can't keep an iterator open
		// and send delete requests. We are forced to pull all results
		// to memory first.
		// TODO: run select with a limit, delete and rerun it again
		for it.Next(ctx) {
			allKeys = append(allKeys, it.Key())
		}
		if err := it.Err(); err != nil {
			return err
		}
	}

	del, err := tbl.tx.prepare(ctx, qu)
	if err != nil {
		return err
	}
	defer del.Close()

	deleteOne := func(args []interface{}) error {
		return tbl.tx.execStmt(ctx, del, args...)
	}

	if deletePullAll { // TODO: should depend on the driver
		for _, key := range allKeys {
			err := deleteOne(tbl.appendKey(nil, key))
			if err != nil {
				return err
			}
		}
		return nil
	}
	for it.Next(ctx) {
		key := it.Key()
		err := deleteOne(tbl.appendKey(nil, key))
		if err != nil {
			return err
		}
	}
	return it.Err()
}

func (tbl *sqlTable) scan(open rowsFunc, f *tuple.Filter) tuple.Iterator {
	return &sqlIterator{tbl: tbl, open: open, f: f}
}

func (tbl *sqlTable) scanWhere(where string, args []interface{}, f *tuple.Filter) tuple.Iterator {
	return tbl.scan(func(ctx context.Context) (*sql.Rows, error) {
		return tbl.tx.query(ctx, `SELECT `+tbl.names()+` FROM `+tbl.name()+where, args...)
	}, f)
}

func (tbl *sqlTable) Scan(f *tuple.Filter) tuple.Iterator {
	where, args, f := tbl.asWhere(f)
	return tbl.scanWhere(where, args, f)
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

package tuple

import (
	"context"
	"errors"
	"fmt"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/filter"
	"github.com/hidal-go/hidalgo/values"
)

var (
	ErrNotFound      = errors.New("tuple: not found")
	ErrTableNotFound = errors.New("tuple: table not found")
	ErrTableExists   = errors.New("tuple: table already exists")
	ErrExists        = errors.New("tuple: this key already exists")
	ErrReadOnly      = errors.New("tuple: read-only database")
)

// Type is any value type that can be stored in tuple.
type Type = values.Type

// Value is any value that can be stored in tuple.
type Value = values.Value

// KeyType is a value type that can be sorted after serialization.
type KeyType = values.SortableType

// Sortable is a value that can be sorted after serialization.
type Sortable = values.Sortable

// Field is a single field used in tuple payload.
type Field struct {
	Name string // field name
	Type Type   // field type
}

// KeyField is a single primary key field used in tuple.
type KeyField struct {
	Name string  // field name
	Type KeyType // field type
	Auto bool    // autoincrement
}

// Header describes a schema of tuples table.
type Header struct {
	Name string     // name of the table
	Key  []KeyField // primary key fields
	Data []Field    // payload fields
}

// KeyByName finds a key field by a name, or returns nil if it not exists.
// It also returns an index in the tuple key.
func (t Header) KeyByName(name string) (*KeyField, int) {
	for i, c := range t.Key {
		if c.Name == name {
			return &c, i
		}
	}
	return nil, -1
}

// DataByName finds a payload field by a name, or returns nil if it not exists.
// It also returns an index in the tuple payload.
func (t Header) DataByName(name string) (*Field, int) {
	for i, c := range t.Data {
		if c.Name == name {
			return &c, i
		}
	}
	return nil, -1
}

// Clone makes a copy of the header.
func (t Header) Clone() Header {
	t.Key = append([]KeyField{}, t.Key...)
	t.Data = append([]Field{}, t.Data...)
	return t
}

// Validate verifies that all fields of header are valid.
func (t Header) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("table name should be set")
	} else if len(t.Key) < 0 {
		return fmt.Errorf("at least one key field is required")
	}
	names := make(map[string]struct{})
	for _, f := range t.Key {
		if f.Name == "" {
			return fmt.Errorf("field name should not be empty")
		} else if f.Type == nil {
			return fmt.Errorf("key type should be specified")
		} else if f.Auto {
			if len(t.Key) != 1 {
				return fmt.Errorf("only one auto key is allowed")
			}
			switch f.Type.(type) {
			case values.UIntType:
			default:
				return fmt.Errorf("only uint types can be autoincremented")
			}
		}
		if _, ok := names[f.Name]; ok {
			return fmt.Errorf("duplicate field name: %q", f.Name)
		}
		names[f.Name] = struct{}{}
	}
	for _, f := range t.Data {
		if f.Name == "" {
			return fmt.Errorf("field name should not be empty")
		} else if f.Type == nil {
			return fmt.Errorf("value type should be specified")
		}
		if _, ok := names[f.Name]; ok {
			return fmt.Errorf("duplicate field name: %q", f.Name)
		}
		names[f.Name] = struct{}{}
	}
	return nil
}

// ValidateKey verifies that specific key is valid for this table.
func (t Header) ValidateKey(k Key, insert bool) error {
	if len(k) == 0 {
		return ErrNotFound
	}
	if len(t.Key) != len(k) {
		return fmt.Errorf("wrong key size: %d vs %d", len(t.Key), len(k))
	}
	for i, f := range t.Key {
		v := k[i]
		if v == nil && (!f.Auto || !insert) {
			return fmt.Errorf("key fields should be set")
		} else if v != nil && v.Type() != f.Type {
			return fmt.Errorf("key %q: expected %T, got %T", f.Name, f.Type, v.Type())
		}
	}
	return nil
}

// ValidatePref verifies that specific key prefix is valid for this table.
func (t Header) ValidatePref(k Key) error {
	for i := range t.Key {
		if i >= len(k) {
			break
		}
		if k[i] == nil {
			return fmt.Errorf("key fields should be set")
		}
		// TODO: type check
	}
	return nil
}

// ValidateData verifies that specific payload is valid for this table.
func (t Header) ValidateData(d Data) error {
	if len(t.Data) != len(d) {
		return fmt.Errorf("wrong payload size")
	}
	for i, f := range t.Data {
		v := d[i]
		if v != nil && v.Type() != f.Type {
			return fmt.Errorf("payload %q: expected %T, got %T", f.Name, f.Type, v.Type())
		}
	}
	return nil
}

// Key is a tuple primary key.
type Key []Sortable

// Compare return 0 when keys are equal, -1 when k < k2 and +1 when k > k2.
func (k Key) Compare(k2 Key) int {
	for i, s := range k {
		if i >= len(k2) {
			return +1
		}
		if d := values.Compare(s, k2[i]); d != 0 {
			return d
		}
	}
	if len(k) < len(k2) {
		return -1
	}
	return 0
}

// SKey creates a string key.
func SKey(key ...string) Key {
	out := make(Key, 0, len(key))
	for _, k := range key {
		out = append(out, values.String(k))
	}
	return out
}

// AutoKey returns an auto-increment key value for insert.
func AutoKey() Key {
	return Key{nil}
}

// Data is a tuple payload.
type Data []Value

// SData creates a string payload.
func SData(data ...string) Data {
	out := make(Data, 0, len(data))
	for _, v := range data {
		out = append(out, values.String(v))
	}
	return out
}

// Tuple is a data tuple.
type Tuple struct {
	Key  Key  // primary key
	Data Data // payload
}

// Store is an interface for tuple stores with a strict schema.
type Store interface {
	base.DB
	// Tx opens a read-only or read-write transaction in the tuple store.
	Tx(rw bool) (Tx, error)
	// View provides functional-style read-only transactional access the tuple store.
	View(func(tx Tx) error) error
	// Update provides functional-style read-write transactional access to the tuple store.
	Update(func(tx Tx) error) error
	// Table returns a table info. It returns ErrTableNotFound if table does not exists.
	// TableInfo can be used to open a Table from transactions more efficiently.
	Table(ctx context.Context, name string) (TableInfo, error)
	// ListTables lists all available tables.
	ListTables(ctx context.Context) ([]TableInfo, error)
}

// UpdateOpt specifies options used in UpdateTuple.
type UpdateOpt struct {
	Upsert bool // create a tuple if it does not exists
}

// Tx is a transaction over a tuple store.
type Tx interface {
	base.Tx
	// Table opens a tuple table. It returns ErrTableNotFound if table does not exists.
	Table(ctx context.Context, name string) (Table, error)
	// ListTables lists all available tables.
	ListTables(ctx context.Context) ([]Table, error)
	// CreateTable creates and opens a table with a specific schema.
	CreateTable(ctx context.Context, table Header) (Table, error)
}

type ScanOptions struct {
	// KeysOnly is a hint for backend to omit fetching keys for an iterator.
	KeysOnly bool
	// Sort is an optional sorting order for a tuple key. Defaults to a native order of the backend.
	Sort Sorting
	// Filter is an optional filter for tuples.
	Filter *Filter
	// Limit limits the maximal number of tuples to return. Limit <= 0 indicates an unlimited number of results.
	Limit int
}

type Scanner interface {
	// Scan iterates over all tuples matching specific parameters.
	Scan(opt *ScanOptions) Iterator
}

// TableInfo represent a metadata of a tuple table.
type TableInfo interface {
	// Header returns a tuple header used in this table.
	Header() Header
	// Open binds a table to the transaction and opens it for further operations.
	Open(tx Tx) (Table, error)
}

// Table represents an opened tuples table with a specific type (schema).
type Table interface {
	TableInfo

	// Drop clears the data and removes the table.
	Drop(ctx context.Context) error
	// Clears removes all tuples from the table.
	Clear(ctx context.Context) error

	// GetTuple fetches one tuple with a specific key.
	// It returns ErrNotFound if tuple does not exists.
	GetTuple(ctx context.Context, key Key) (Data, error)
	// GetTupleBatch fetches multiple tuples with provided keys.
	// Nil values in the returned slice indicates that specific key does not exists.
	GetTupleBatch(ctx context.Context, keys []Key) ([]Data, error)
	// InsertTuple creates a new tuple. If the tuple with specified key already exists it returns ErrExists.
	InsertTuple(ctx context.Context, t Tuple) (Key, error)
	// UpdateTuple rewrites specified tuple. Options can be provided to create a tuple if ti does not exists.
	// If this flag is not provided and the tuple is missing, it returns ErrNotFound.
	UpdateTuple(ctx context.Context, t Tuple, opt *UpdateOpt) error
	// DeleteTuples removes all tuples that matches a filter.
	DeleteTuples(ctx context.Context, f *Filter) error
	Scanner
}

type Sorting int

const (
	SortAsc  = Sorting(+1)
	SortAny  = Sorting(0)
	SortDesc = Sorting(-1)
)

// Iterator is an iterator over a tuple store.
type Iterator interface {
	base.Iterator
	// Key returns a primary key of the tuple.
	Key() Key
	// Data returns a payload the tuple.
	Data() Data
}

// Filter is a tuple filter.
type Filter struct {
	KeyFilter
	DataFilter
}

func (f *Filter) IsAny() bool {
	return f == nil || (f.KeyFilter == nil && f.DataFilter == nil)
}
func (f *Filter) IsAnyKey() bool {
	return f == nil || f.KeyFilter == nil
}
func (f *Filter) IsAnyData() bool {
	return f == nil || f.DataFilter == nil
}
func (f *Filter) FilterKey(k Key) bool {
	if f.IsAnyKey() {
		return true
	}
	return f.KeyFilter.FilterKey(k)
}
func (f *Filter) FilterData(d Data) bool {
	if f.IsAnyData() {
		return true
	}
	return f.DataFilter.FilterData(d)
}
func (f *Filter) FilterTuple(t Tuple) bool {
	if f == nil {
		return true
	}
	if f.KeyFilter != nil && !f.KeyFilter.FilterKey(t.Key) {
		return false
	}
	if f.DataFilter != nil && !f.DataFilter.FilterData(t.Data) {
		return false
	}
	return true
}

// KeyFilter controls if a key should be considered or not.
type KeyFilter interface {
	FilterKey(k Key) bool
}

// Keys is a filter that accepts only specific keys.
type Keys []Key

func (arr Keys) FilterKey(k Key) bool {
	for _, k2 := range arr {
		if k.Compare(k2) == 0 {
			return true
		}
	}
	return true
}

// KeyFilters applies value filters to individual key components.
// If key is shorter, nil value is passed to the filter.
type KeyFilters []filter.ValueFilter

func (arr KeyFilters) FilterKey(k Key) bool {
	for i, f := range arr {
		var v Sortable
		if i < len(k) {
			v = k[i]
		}
		if !f.FilterValue(v) {
			return false
		}
	}
	return true
}

// DataFilter controls if a payload should be considered or not.
type DataFilter interface {
	FilterData(d Data) bool
}

// DataFilters applies value filters to individual payload components.
// Filter will reject the value if its length is different.
// Nil filters are allowed in the slice to indicate no filtering.
type DataFilters []filter.ValueFilter

func (arr DataFilters) FilterData(d Data) bool {
	if len(d) != len(arr) {
		return false
	}
	for i, f := range arr {
		if f == nil {
			continue
		}
		v := d[i]
		if !f.FilterValue(v) {
			return false
		}
	}
	return true
}

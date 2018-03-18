package tuple

import (
	"context"
	"errors"
	"fmt"

	"github.com/nwca/uda/base"
	"github.com/nwca/uda/types"
)

var (
	ErrNotFound      = errors.New("tuple: not found")
	ErrTableNotFound = errors.New("tuple: table not found")
	ErrExists        = errors.New("tuple: this key already exists")
	ErrReadOnly      = errors.New("tuple: not found")
)

// Type is any value type that can be stored in tuple.
type Type = types.Type

// Value is any value that can be stored in tuple.
type Value = types.Value

// KeyType is a value type that can be sorted after serialization.
type KeyType = types.SortableType

// Sortable is a value type that can be sorted after serialization.
type Sortable = types.Sortable

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
			case types.UIntType:
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
		if k[i] == nil && (!f.Auto || !insert) {
			return fmt.Errorf("key fields should be set")
		}
		// TODO: type check
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
		return fmt.Errorf("wrong key size")
	}
	// TODO: type check
	return nil
}

// Key is a tuple primary key.
type Key []Sortable

// Data is a tuple payload.
type Data []Value

// Tuple is a data tuple.
type Tuple struct {
	Key  Key  // primary key
	Data Data // payload
}

// Store is an interface for tuple stores with a strict schema.
type Store interface {
	base.DB
	Tx(rw bool) (Tx, error)
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

// Table represents an opened tuples table with a specific type (schema).
type Table interface {
	// Delete clears the data and removes the table.
	Delete(ctx context.Context) error
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
	// DeleteTuple removes a tuple with specific key.
	DeleteTuple(ctx context.Context, key Key) error
	// Scan iterates over all tuples with a specific key prefix.
	Scan(pref Key) Iterator
}

// Iterator is an iterator over a tuple store.
type Iterator interface {
	base.Iterator
	// Key returns a primary key of the tuple.
	Key() Key
	// Data returns a payload the tuple.
	Data() Data
}

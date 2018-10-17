package base

import (
	"github.com/dennwc/base"
)

// DB is a common interface implemented by all database abstractions.
type DB interface {
	// Close closes the database.
	Close() error
}

// Tx is a common interface implemented by all transactions.
type Tx = base.Tx

// Iterator is a common interface implemented by all iterators.
type Iterator = base.IteratorContext

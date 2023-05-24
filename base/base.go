package base

import "context"

// DB is a common interface implemented by all database abstractions.
type DB interface {
	// Close closes the database.
	Close() error
}

// Tx is a common interface implemented by all transactions.
type Tx interface {
	// Commit applies all changes made in the transaction.
	Commit(ctx context.Context) error
	// Close rolls back the transaction.
	// Committed transactions will not be affected by calling Close.
	Close() error
}

// Iterator is a common interface implemented by all iterators.
type Iterator interface {
	// Next advances an iterator.
	Next(ctx context.Context) bool
	// Err returns a last encountered error.
	Err() error
	// Close frees resources.
	Close() error
}

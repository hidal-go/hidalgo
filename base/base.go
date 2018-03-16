package base

import "context"

type DB interface {
	Close() error
}

type Tx interface {
	Commit(ctx context.Context) error
	Close() error
}

type Iterator interface {
	Next(ctx context.Context) bool
	Err() error
	Close() error
}

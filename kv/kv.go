package kv

import (
	"context"
	"errors"
)

var (
	ErrNotFound     = errors.New("kv: not found")
	ErrNoBucket     = errors.New("kv: bucket is missing")
	ErrBucketExists = errors.New("kv: bucket already exists")
)

type Tx interface {
	Commit(ctx context.Context) error
	Rollback() error
}

type Bucket interface {
	Get(ctx context.Context, keys [][]byte) ([][]byte, error)
	Put(k, v []byte) error
	Del(k []byte) error
	Scan(pref []byte) Iterator
}

func GetOne(ctx context.Context, b Bucket, key []byte) ([]byte, error) {
	out, err := b.Get(ctx, [][]byte{key})
	if err != nil {
		return nil, err
	} else if len(out) == 0 || out[0] == nil {
		return nil, ErrNotFound
	}
	return out[0], nil
}

type Iterator interface {
	Next(ctx context.Context) bool
	Err() error
	Close() error
	Key() []byte
	Val() []byte
}

type BucketKey struct {
	Bucket, Key []byte
}

type BucketTx interface {
	Tx
	Bucket(name []byte) Bucket
	Get(ctx context.Context, keys []BucketKey) ([][]byte, error)
}

type Base interface {
	Type() string
	Close() error
}

type DB interface {
	Base
	Tx(update bool) (BucketTx, error)
}

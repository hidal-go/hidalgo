package kv

import "context"

func Update(ctx context.Context, kv DB, update func(tx BucketTx) error) error {
	tx, err := kv.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = update(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func View(kv DB, view func(tx BucketTx) error) error {
	tx, err := kv.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = view(tx)
	if err == nil {
		err = tx.Rollback()
	}
	return err
}

func Each(ctx context.Context, b Bucket, pref []byte, fnc func(k, v []byte) error) error {
	it := b.Scan(pref)
	defer it.Close()
	for it.Next(ctx) {
		if err := fnc(it.Key(), it.Val()); err != nil {
			return err
		}
	}
	return it.Err()
}

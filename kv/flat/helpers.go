package flat

import "context"

// Update is a helper to open a read-write transaction and update the database.
// The update function may be called multiple times in case of conflicts with other writes.
func Update(ctx context.Context, kv KV, update func(tx Tx) error) error {
	for {
		err := func() error {
			tx, err := kv.Tx(ctx, true)
			if err != nil {
				return err
			}
			defer tx.Close()
			err = update(tx)
			if err != nil {
				return err
			}
			return tx.Commit(ctx)
		}()
		if err == ErrConflict {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
}

// View is a helper to open a read-only transaction to read the database.
func View(ctx context.Context, kv KV, view func(tx Tx) error) error {
	tx, err := kv.Tx(ctx, false)
	if err != nil {
		return err
	}
	if err = view(tx); err != nil {
		defer tx.Close()
		return err
	}
	return tx.Close()
}

// Each is a helper to enumerate all key-value pairs with a specific prefix.
// See Iterator for rules of using returned values.
func Each(ctx context.Context, tx Tx, fnc func(k Key, v Value) error, opts ...IteratorOption) error {
	it := tx.Scan(ctx, opts...)
	defer it.Close()
	for it.Next(ctx) {
		if err := fnc(it.Key(), it.Val()); err != nil {
			return err
		}
	}
	return it.Err()
}

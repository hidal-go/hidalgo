package kv

import "context"

// Update is a helper to open a read-write transaction and update the database.
func Update(ctx context.Context, kv KV, update func(tx Tx) error) error {
	tx, err := kv.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err = update(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// View is a helper to open a read-only transaction to read the database.
func View(kv KV, view func(tx Tx) error) error {
	tx, err := kv.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Close()
	err = view(tx)
	if err == nil {
		err = tx.Close()
	}
	return err
}

// Each is a helper to to enumerate all key-value pairs with a specific prefix.
// See Iterator for rules of using returned values.
func Each(ctx context.Context, tx Tx, pref Key, fnc func(k Key, v Value) error) error {
	it := tx.Scan(pref)
	defer it.Close()
	for it.Next(ctx) {
		if err := fnc(it.Key(), it.Val()); err != nil {
			return err
		}
	}
	return it.Err()
}

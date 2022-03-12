package flat

import "context"

// Update is a helper to open a read-write transaction and update the database.
func Update(ctx context.Context, kv KV, update func(tx Tx) error) error {
	tx, err := kv.Tx(true)
	if err != nil {
		return err
	}
	if err = update(tx); err != nil {
		defer tx.Close()
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		defer tx.Close()
		return err
	}
	return tx.Close()
}

// View is a helper to open a read-only transaction to read the database.
func View(ctx context.Context, kv KV, view func(tx Tx) error) error {
	tx, err := kv.Tx(false)
	if err != nil {
		return err
	}
	if err = view(tx); err != nil {
		defer tx.Close()
		return err
	}
	return tx.Close()
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

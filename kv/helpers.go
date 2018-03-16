package kv

import "context"

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

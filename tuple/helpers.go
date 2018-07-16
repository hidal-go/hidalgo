package tuple

type LazyTuple interface {
	Key() Key
	Data() Data
}

func FilterIterator(it LazyTuple, f *Filter, next func() bool) bool {
	if f.IsAny() {
		return next()
	}
	for next() {
		if f.KeyFilter != nil && !f.KeyFilter.FilterKey(it.Key()) {
			continue
		}
		if f.DataFilter != nil && !f.DataFilter.FilterData(it.Data()) {
			continue
		}
		return true
	}
	return false
}

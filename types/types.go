package types

type Type interface {
	// New creates a new zero value of this type.
	New() Value
}

type SortableType interface {
	Type
	// NewSortable creates a new zero value of this type.
	NewSortable() Sortable
}

type BytesType struct{}

func (BytesType) New() Value {
	var v Bytes
	return &v
}
func (BytesType) NewSortable() Sortable {
	var v Bytes
	return &v
}

type StringType struct{}

func (StringType) New() Value {
	var v String
	return &v
}
func (StringType) NewSortable() Sortable {
	var v String
	return &v
}

type IntType struct{}

func (IntType) New() Value {
	var v Int
	return &v
}
func (IntType) NewSortable() Sortable {
	var v Int
	return &v
}

type UIntType struct{}

func (UIntType) New() Value {
	var v UInt
	return &v
}
func (UIntType) NewSortable() Sortable {
	var v UInt
	return &v
}

type BoolType struct{}

func (BoolType) New() Value {
	var v Bool
	return &v
}
func (BoolType) NewSortable() Sortable {
	var v Bool
	return &v
}

type TimeType struct{}

func (TimeType) New() Value {
	var v Time
	return &v
}
func (TimeType) NewSortable() Sortable {
	var v Time
	return &v
}

type FloatType struct{}

func (FloatType) New() Value {
	var v Float
	return &v
}

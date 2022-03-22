package values

type PrimitiveType interface {
	Type
	// NewPrimitive creates a new zero value of this type.
	NewPrimitive() PrimitiveDest
}

type Type interface {
	// New creates a new zero value of this type.
	New() ValueDest
}

type SortableType interface {
	Type
	// NewSortable creates a new zero value of this type.
	NewSortable() SortableDest
}

type BytesType struct{}

func (tp BytesType) New() ValueDest {
	return tp.NewSortable()
}

func (BytesType) NewSortable() SortableDest {
	return new(Bytes)
}

func (BytesType) NewPrimitive() PrimitiveDest {
	return new(Bytes)
}

type StringType struct{}

func (tp StringType) New() ValueDest {
	return tp.NewSortable()
}

func (StringType) NewSortable() SortableDest {
	return new(String)
}

func (StringType) NewPrimitive() PrimitiveDest {
	return new(String)
}

type IntType struct{}

func (tp IntType) New() ValueDest {
	return tp.NewSortable()
}

func (IntType) NewSortable() SortableDest {
	return new(Int)
}

func (IntType) NewPrimitive() PrimitiveDest {
	return new(Int)
}

type UIntType struct{}

func (tp UIntType) New() ValueDest {
	return tp.NewSortable()
}

func (UIntType) NewSortable() SortableDest {
	return new(UInt)
}

func (UIntType) NewPrimitive() PrimitiveDest {
	return new(UInt)
}

type BoolType struct{}

func (tp BoolType) New() ValueDest {
	return tp.NewSortable()
}

func (BoolType) NewSortable() SortableDest {
	return new(Bool)
}

func (BoolType) NewPrimitive() PrimitiveDest {
	return new(Bool)
}

type TimeType struct{}

func (tp TimeType) New() ValueDest {
	return tp.NewSortable()
}

func (TimeType) NewSortable() SortableDest {
	return new(Time)
}

type FloatType struct{}

func (FloatType) New() ValueDest {
	return new(Float)
}

func (FloatType) NewPrimitive() PrimitiveDest {
	return new(Float)
}

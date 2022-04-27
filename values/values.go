package values

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type Value interface {
	// Native returns a native Go value represented by this type.
	Native() interface{}
	// Type returns a type associated with this value.
	Type() Type
	// Marshal implementations should not include the length of the value.
	encoding.BinaryMarshaler
}

type ValueDest interface {
	Value() Value
	NativePtr() interface{}
	encoding.BinaryUnmarshaler
}

// Primitive is a private interface implemented only by following types:
//
//	* String
//	* Bytes
//	* Int
//	* UInt
//	* Float
//	* Bool
type Primitive interface {
	Value
	// PrimitiveType returns a type associated with this value.
	PrimitiveType() PrimitiveType
	isPrimitive()
}

type PrimitiveDest interface {
	ValueDest
	Primitive() Primitive
}

// Sortable is an extension of Value interface that allows to use it in building low-level indexes.
type Sortable interface {
	Value
	// Compare returns 0 if x == v, -1 if x < v and +1 if x > v
	Compare(v Sortable) int
	// SortableType returns a type associated with this value.
	SortableType() SortableType
	// MarshalSortable encodes the value into sortable encoding: v1 < v2, marshal(v1) < marshal(v2).
	MarshalSortable() ([]byte, error)
}

type BinaryString interface {
	Sortable
	// PrefixEnd returns the next binary key that ends this prefix.
	PrefixEnd() BinaryString
}

type SortableDest interface {
	ValueDest
	Sortable() Sortable
	// UnmarshalSortable decodes the value from sortable encoding.
	UnmarshalSortable(p []byte) error
}

func Compare(a, b Sortable) int {
	if a != nil {
		return a.Compare(b)
	}
	if b != nil {
		return -b.Compare(a)
	}
	return 0
}

var (
	sortableOrder = binary.BigEndian
	defaultOrder  = binary.LittleEndian
)

var (
	_ PrimitiveDest = (*Int)(nil)
	_ PrimitiveDest = (*UInt)(nil)
	_ PrimitiveDest = (*String)(nil)
	_ PrimitiveDest = (*Bytes)(nil)
	_ PrimitiveDest = (*Bool)(nil)
	_ PrimitiveDest = (*Float)(nil)

	_ SortableDest = (*UInt)(nil)
	_ SortableDest = (*String)(nil)
	_ SortableDest = (*Bytes)(nil)
	_ SortableDest = (*Bool)(nil)
	_ SortableDest = (*Time)(nil)

	_ ValueDest = (*Int)(nil)
	_ ValueDest = (*Float)(nil)

	_ BinaryString = String("")
	_ BinaryString = Bytes{}
)

type String string

func (String) isPrimitive() {}
func (v String) Native() interface{} {
	return string(v)
}

func (v *String) NativePtr() interface{} {
	return (*string)(v)
}

func (String) Type() Type {
	return StringType{}
}

func (String) SortableType() SortableType {
	return StringType{}
}

func (String) PrimitiveType() PrimitiveType {
	return StringType{}
}

func (v *String) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *String) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return *v
}

func (v *String) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v String) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

// PrefixEnd returns the next binary key that ends the prefix.
func (v String) PrefixEnd() BinaryString {
	end := prefixEnd([]byte(v))
	if end == nil {
		return nil
	}
	return String(end)
}

func (v String) MarshalBinary() ([]byte, error) {
	return []byte(v), nil
}

func (v *String) UnmarshalBinary(p []byte) error {
	*v = String(p)
	return nil
}

func (v String) MarshalSortable() ([]byte, error) {
	return []byte(v), nil
}

func (v *String) UnmarshalSortable(p []byte) error {
	*v = String(p)
	return nil
}

type Bytes []byte

func (Bytes) isPrimitive() {}
func (v Bytes) Native() interface{} {
	return []byte(v)
}

func (v *Bytes) NativePtr() interface{} {
	return (*[]byte)(v)
}

func (Bytes) Type() Type {
	return BytesType{}
}

func (Bytes) SortableType() SortableType {
	return BytesType{}
}

func (Bytes) PrimitiveType() PrimitiveType {
	return BytesType{}
}

func (v *Bytes) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Bytes) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Bytes) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v Bytes) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

func prefixEnd(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] < 0xff {
			key[i]++
			return key[:i+1]
		}
	}
	// next prefix does not exist (e.g., 0xffff)
	return nil
}

// PrefixEnd returns the next binary key that ends the prefix.
func (v Bytes) PrefixEnd() BinaryString {
	end := prefixEnd(append([]byte{}, v...))
	if end == nil {
		return nil
	}
	return Bytes(end)
}

func (v Bytes) MarshalBinary() ([]byte, error) {
	return append([]byte{}, v...), nil
}

func (v *Bytes) UnmarshalBinary(p []byte) error {
	*v = Bytes(append([]byte{}, p...))
	return nil
}

func (v Bytes) MarshalSortable() ([]byte, error) {
	return append([]byte{}, v...), nil
}

func (v *Bytes) UnmarshalSortable(p []byte) error {
	*v = Bytes(append([]byte{}, p...))
	return nil
}

type Int int64

func (Int) isPrimitive() {}
func (v Int) Native() interface{} {
	return int64(v)
}

func (v *Int) NativePtr() interface{} {
	return (*int64)(v)
}

func (Int) Type() Type {
	return IntType{}
}

func (Int) SortableType() SortableType {
	return IntType{}
}

func (Int) PrimitiveType() PrimitiveType {
	return IntType{}
}

func (v *Int) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Int) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Int) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v Int) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

func (v Int) MarshalBinary() ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(v))
	return buf[:n], nil
}

func (v *Int) UnmarshalBinary(p []byte) error {
	iv, n := binary.Varint(p)
	if n == 0 {
		return io.ErrUnexpectedEOF
	} else if n < 0 {
		return errors.New("int overflow")
	} else if n < len(p) {
		return errors.New("unexpected data")
	}
	*v = Int(iv)
	return nil
}

const uintShift = -math.MinInt64

func (v Int) asSortable() uint64 {
	if v >= 0 {
		return uint64(v) + uintShift
	}
	iv := uint64(-v)
	return uintShift - iv
}

func (v *Int) setSortable(uv uint64) {
	if uv >= uintShift {
		*v = Int(uv - uintShift)
	} else {
		uv = uintShift - uv
		*v = Int(-int64(uv))
	}
}

func (v Int) MarshalSortable() ([]byte, error) {
	buf := make([]byte, 8)
	sortableOrder.PutUint64(buf, v.asSortable())
	return buf, nil
}

func (v *Int) UnmarshalSortable(p []byte) error {
	if len(p) != 8 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	v.setSortable(sortableOrder.Uint64(p))
	return nil
}

type UInt uint64

func (UInt) isPrimitive() {}
func (v UInt) Native() interface{} {
	return uint64(v)
}

func (v *UInt) NativePtr() interface{} {
	return (*uint64)(v)
}

func (UInt) Type() Type {
	return UIntType{}
}

func (UInt) SortableType() SortableType {
	return UIntType{}
}

func (UInt) PrimitiveType() PrimitiveType {
	return UIntType{}
}

func (v *UInt) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *UInt) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return *v
}

func (v *UInt) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v UInt) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

func (v UInt) MarshalBinary() ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(v))
	return buf[:n], nil
}

func (v *UInt) UnmarshalBinary(p []byte) error {
	iv, n := binary.Uvarint(p)
	if n == 0 {
		return io.ErrUnexpectedEOF
	} else if n < 0 {
		return errors.New("int overflow")
	} else if n < len(p) {
		return errors.New("unexpected data")
	}
	*v = UInt(iv)
	return nil
}

func (v UInt) MarshalSortable() ([]byte, error) {
	buf := make([]byte, 8)
	sortableOrder.PutUint64(buf, uint64(v))
	return buf, nil
}

func (v *UInt) UnmarshalSortable(p []byte) error {
	if len(p) != 8 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	*v = UInt(sortableOrder.Uint64(p))
	return nil
}

type Float float64

func (Float) isPrimitive() {}
func (v Float) Native() interface{} {
	return float64(v)
}

func (v *Float) NativePtr() interface{} {
	return (*float64)(v)
}

func (Float) Type() Type {
	return FloatType{}
}

func (Float) PrimitiveType() PrimitiveType {
	return FloatType{}
}

func (v *Float) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Float) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v Float) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8)
	iv := math.Float64bits(float64(v))
	defaultOrder.PutUint64(buf, iv)
	return buf, nil
}

func (v *Float) UnmarshalBinary(p []byte) error {
	if len(p) != 8 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	iv := defaultOrder.Uint64(p)
	*v = Float(math.Float64frombits(iv))
	return nil
}

type Bool bool

func (Bool) isPrimitive() {}
func (v Bool) Native() interface{} {
	return bool(v)
}

func (v *Bool) NativePtr() interface{} {
	return (*bool)(v)
}

func (Bool) Type() Type {
	return BoolType{}
}

func (Bool) SortableType() SortableType {
	return BoolType{}
}

func (Bool) PrimitiveType() PrimitiveType {
	return BoolType{}
}

func (v *Bool) Value() Value {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Bool) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return *v
}

func (v *Bool) Primitive() Primitive {
	if v == nil {
		return nil
	}
	return *v
}

func (v Bool) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

func (v Bool) MarshalBinary() ([]byte, error) {
	if v {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (v *Bool) UnmarshalBinary(p []byte) error {
	if len(p) != 1 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	*v = Bool(p[0] != 0)
	return nil
}

func (v Bool) MarshalSortable() ([]byte, error) {
	if v {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (v *Bool) UnmarshalSortable(p []byte) error {
	if len(p) != 1 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	*v = Bool(p[0] != 0)
	return nil
}

func AsTime(t time.Time) Time {
	return Time(t.UTC())
}

type Time time.Time

func (v Time) Native() interface{} {
	return time.Time(v)
}

func (v Time) String() string {
	return time.Time(v).String()
}

func (v *Time) NativePtr() interface{} {
	return (*time.Time)(v)
}

func (Time) Type() Type {
	return TimeType{}
}

func (Time) SortableType() SortableType {
	return TimeType{}
}

func (v *Time) Value() Value {
	if v == nil {
		return nil
	}
	return Time(time.Time(*v).UTC())
}

func (v *Time) Sortable() Sortable {
	if v == nil {
		return nil
	}
	return Time(time.Time(*v).UTC())
}

func (v Time) Compare(b Sortable) int {
	if b == nil {
		return +1
	}
	// TODO: optimize
	ab, _ := v.MarshalSortable()
	bb, _ := b.MarshalSortable()
	return bytes.Compare(ab, bb)
}

func (v Time) MarshalBinary() ([]byte, error) {
	return time.Time(v).MarshalBinary()
}

func (v *Time) UnmarshalBinary(p []byte) error {
	var t time.Time
	if err := t.UnmarshalBinary(p); err != nil {
		return err
	}
	*v = AsTime(t)
	return nil
}

func (v Time) MarshalSortable() ([]byte, error) {
	iv := Int(time.Time(v).UnixNano())
	return iv.MarshalSortable()
}

func (v *Time) UnmarshalSortable(p []byte) error {
	var iv Int
	if err := iv.UnmarshalSortable(p); err != nil {
		return err
	}
	*v = Time(time.Unix(0, int64(iv)).UTC())
	return nil
}

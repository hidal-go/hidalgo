package types

import (
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
	// Zero creates a new zero value of the same type.
	Zero() Value
	// Marshal implementations should not include the length of the value.
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Sortable is an extension of Value interface that allows to use it in building low-level indexes.
type Sortable interface {
	Value
	// MarshalSortable encodes the value into sortable encoding: v1 < v2, marshal(v1) < marshal(v2).
	MarshalSortable() ([]byte, error)
	// UnmarshalSortable decodes the value from sortable encoding.
	UnmarshalSortable(p []byte) error
}

var (
	sortableOrder = binary.BigEndian
	defaultOrder  = binary.LittleEndian
)

var (
	_ Sortable = (*Int)(nil)
	_ Sortable = (*UInt)(nil)
	_ Sortable = (*String)(nil)
	_ Sortable = (*Bytes)(nil)
	_ Sortable = (*Bool)(nil)
	_ Sortable = (*Time)(nil)

	_ Value = (*Float)(nil)
)

type String string

func (v String) Native() interface{} {
	return string(v)
}
func (v String) Zero() Value {
	v2 := String("")
	return &v2
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

func (v Bytes) Native() interface{} {
	return []byte(v)
}
func (v Bytes) Zero() Value {
	v2 := Bytes{}
	return &v2
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

func (v Int) Native() interface{} {
	return int64(v)
}
func (v Int) Zero() Value {
	v2 := Int(0)
	return &v2
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
func (v Int) MarshalSortable() ([]byte, error) {
	buf := make([]byte, 8)
	sortableOrder.PutUint64(buf, uint64(v))
	return buf, nil
}
func (v *Int) UnmarshalSortable(p []byte) error {
	if len(p) != 8 {
		return fmt.Errorf("unexpected value size: %d", len(p))
	}
	*v = Int(sortableOrder.Uint64(p))
	return nil
}

type UInt uint64

func (v UInt) Native() interface{} {
	return uint64(v)
}
func (v UInt) Zero() Value {
	v2 := UInt(0)
	return &v2
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

func (v Float) Native() interface{} {
	return float64(v)
}
func (v Float) Zero() Value {
	v2 := Float(0)
	return &v2
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

func (v Bool) Native() interface{} {
	return bool(v)
}
func (v Bool) Zero() Value {
	v2 := Bool(false)
	return &v2
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

type Time time.Time

func (v Time) Native() interface{} {
	return time.Time(v)
}
func (v Time) Zero() Value {
	v2 := Time{}
	return &v2
}
func (v Time) MarshalBinary() ([]byte, error) {
	return time.Time(v).MarshalBinary()
}
func (v *Time) UnmarshalBinary(p []byte) error {
	var t time.Time
	if err := t.UnmarshalBinary(p); err != nil {
		return err
	}
	*v = Time(t)
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
	*v = Time(time.Unix(0, int64(iv)))
	return nil
}

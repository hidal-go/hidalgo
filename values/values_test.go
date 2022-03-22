package values

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var casesType = []struct {
	v Value
	n interface{}
}{
	{v: Bool(false), n: false},
	{v: Int(0), n: int64(0)},
	{v: UInt(0), n: uint64(0)},
	{v: Float(0), n: float64(0)},
	{v: String(""), n: ""},
	{v: Bytes(nil), n: ([]byte)(nil)},
	{v: Time{}, n: time.Time{}},
}

func zeroOf(o interface{}) interface{} {
	return reflect.Zero(reflect.TypeOf(o)).Interface()
}

func TestTypes(t *testing.T) {
	for _, c := range casesType {
		t.Run(fmt.Sprintf("%T", c.v), func(t *testing.T) {
			require.Equal(t, c.n, c.v.Native())
			v2 := c.v.Type().New().Value()
			require.Equal(t, c.v, v2)
			v2 = zeroOf(c.v.Type().New()).(ValueDest).Value()
			require.Equal(t, nil, v2)

			if p, ok := c.v.(Primitive); ok {
				p2 := p.PrimitiveType().NewPrimitive().Primitive()
				require.Equal(t, p, p2)
				p2 = zeroOf(p.PrimitiveType().NewPrimitive()).(PrimitiveDest).Primitive()
				require.Equal(t, nil, p2)
			}

			if s, ok := c.v.(Sortable); ok {
				s2 := s.SortableType().NewSortable().Sortable()
				require.Equal(t, s, s2)
				s2 = zeroOf(s.SortableType().NewSortable()).(SortableDest).Sortable()
				require.Equal(t, nil, s2)
			}
		})
	}
}

var casesMarshalBinary = []struct {
	v Value
}{
	{v: Int(0)},
	{v: Int(-1)},
	{v: Int(+1)},
	{v: Int(math.MinInt64)},
	{v: Int(math.MaxInt64)},
	{v: UInt(0)},
	{v: UInt(+1)},
	{v: UInt(math.MaxUint64)},
	{v: Float(0)},
	{v: Float(+1)},
	{v: Float(-1)},
	{v: Float(math.MaxFloat64)},
	{v: Bool(false)},
	{v: Bool(true)},
	{v: String("")},
	{v: String("abc")},
	{v: Bytes{}},
	{v: Bytes("\x00abc")},
	{v: Time(time.Now().UTC())},
}

func TestMarshalBinary(t *testing.T) {
	for _, c := range casesMarshalBinary {
		t.Run(fmt.Sprintf("%T(%v)", c.v, c.v), func(t *testing.T) {
			data, err := c.v.MarshalBinary()
			require.NoError(t, err)
			v2 := c.v.Type().New()
			err = v2.UnmarshalBinary(data)
			require.NoError(t, err)
			require.Equal(t, c.v, v2.Value())

			if s, ok := c.v.(Sortable); ok {
				data, err = s.MarshalSortable()
				require.NoError(t, err)
				s2 := s.SortableType().NewSortable()
				err = s2.UnmarshalSortable(data)
				require.NoError(t, err)
				require.Equal(t, s, s2.Value())
			}
		})
	}
}

var casesCompare = []struct {
	name string
	a, b Sortable
	exp  int
}{
	{name: "nil == nil", a: nil, b: nil, exp: 0},
	{name: "nil < false", a: nil, b: Bool(false), exp: -1},
	{name: "false < true", a: Bool(false), b: Bool(true), exp: -1},
	{name: "nil < 1", a: nil, b: Int(1), exp: -1},
	{name: "1 > nil", a: Int(1), b: nil, exp: +1},
	{name: "1 == 1", a: Int(1), b: Int(1), exp: 0},
	{name: "1 < 2", a: Int(1), b: Int(2), exp: -1},
	{name: "-1 < 1", a: Int(-1), b: Int(+1), exp: -1},
	{name: "-1 < 0", a: Int(-1), b: Int(0), exp: -1},
	{name: "0 < 1", a: Int(0), b: Int(1), exp: -1},
	{name: "min < min+1", a: Int(math.MinInt64), b: Int(math.MinInt64 + 1), exp: -1},
	{name: "max-1 < max", a: Int(math.MaxInt64 - 1), b: Int(math.MaxInt64), exp: -1},
	{name: "min < max", a: Int(math.MinInt64), b: Int(math.MaxInt64), exp: -1},
	{name: "0 < umax", a: UInt(0), b: UInt(math.MaxUint64), exp: -1},
	{name: "nil < a", a: nil, b: String("a"), exp: -1},
	{name: "a > nil", a: String("a"), b: nil, exp: +1},
	{name: "a == a", a: String("a"), b: String("a"), exp: 0},
	{name: "a < b", a: String("a"), b: String("b"), exp: -1},
	{name: "a < aa", a: String("a"), b: String("aa"), exp: -1},
	{name: "nil < [a]", a: nil, b: Bytes("a"), exp: -1},
	{name: "[] < [a]", a: Bytes{}, b: Bytes("a"), exp: -1},
	{name: "[a] == [a]", a: Bytes("a"), b: Bytes("a"), exp: 0},
	{name: "[a] < [b]", a: Bytes("a"), b: Bytes("b"), exp: -1},
	{name: "[a] < [aa]", a: Bytes("a"), b: Bytes("aa"), exp: -1},
	{name: "nil < t0", a: nil, b: Time{}, exp: -1},
	{name: "t0-1 < t0", a: Time(time.Time{}.Add(-time.Hour)), b: Time{}, exp: -1},
	{name: "now < now+1", a: AsTime(time.Now()), b: AsTime(time.Now().Add(time.Hour)), exp: -1},
}

func TestCompare(t *testing.T) {
	for _, c := range casesCompare {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.exp, Compare(c.a, c.b))
		})
	}
}

func TestIntSortable(t *testing.T) {
	cases := []struct {
		name string
		v    int64
		exp  uint64
	}{
		{"min", math.MinInt64, 0},
		{"min+1", math.MinInt64 + 1, 1},
		{"0", 0, math.MaxInt64 + 1},
		{"max-1", math.MaxInt64 - 1, math.MaxUint64 - 1},
		{"max", math.MaxInt64, math.MaxUint64},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uv := Int(c.v).asSortable()
			require.Equal(t, c.exp, uv, "conversion failed")
			var got Int
			got.setSortable(uv)
			require.Equal(t, c.v, int64(got), "reverse failed")
		})
	}
}

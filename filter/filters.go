package filter

import (
	"bytes"

	"github.com/hidal-go/hidalgo/values"
)

type ValueFilter interface {
	FilterValue(v values.Value) bool
}

type SortableFilter interface {
	ValueFilter
	FilterSortable(v values.Sortable) bool
	// ValuesRange returns an optional range of value that matches the filter.
	// It is used as an optimization for complex filters for backend to limit the range of keys that will be considered.
	ValuesRange() *Range
}

var _ SortableFilter = Any{}

type Any struct{}

func (Any) FilterValue(v values.Value) bool {
	return v != nil
}

func (Any) FilterSortable(v values.Sortable) bool {
	return v != nil
}

func (Any) ValuesRange() *Range {
	return nil
}

// EQ is a shorthand for Equal.
func EQ(v values.Value) SortableFilter {
	return Equal{Value: v}
}

var _ SortableFilter = Equal{}

type Equal struct {
	Value values.Value
}

func (f Equal) FilterValue(a values.Value) bool {
	switch a := a.(type) {
	case values.Bytes:
		b, ok := f.Value.(values.Bytes)
		if !ok {
			return false
		}
		return bytes.Equal(a, b)
	}
	return f.Value == a
}

func (f Equal) FilterSortable(a values.Sortable) bool {
	b, ok := f.Value.(values.Sortable)
	if !ok {
		return a == nil && f.Value == nil
	}

	switch a := a.(type) {
	case values.Bytes:
		b, ok := b.(values.Bytes)
		if !ok {
			return false
		}
		return bytes.Equal(a, b)
	}

	return f.Value == a
}

func (f Equal) ValuesRange() *Range {
	b, ok := f.Value.(values.Sortable)
	if !ok {
		return nil
	}
	return &Range{
		Start: GTE(b),
		End:   LTE(b),
	}
}

// LT is a "less than" filter. Shorthand for Less.
func LT(v values.Sortable) *Less {
	return &Less{Value: v}
}

// LTE is a "less than or equal" filter. Shorthand for Less.
func LTE(v values.Sortable) *Less {
	return &Less{Value: v, Equal: true}
}

var _ SortableFilter = Less{}

type Less struct {
	Value values.Sortable
	Equal bool
}

func (f Less) FilterValue(v values.Value) bool {
	a, ok := v.(values.Sortable)
	if !ok && v != nil {
		return false
	}
	return f.FilterSortable(a)
}

func (f Less) FilterSortable(v values.Sortable) bool {
	if v == nil {
		return true
	}
	c := values.Compare(v, f.Value)
	return c == -1 || (f.Equal && c == 0)
}

func (f Less) ValuesRange() *Range {
	return &Range{End: &f}
}

// GT is a "greater than" filter. Shorthand for Greater.
func GT(v values.Sortable) *Greater {
	return &Greater{Value: v}
}

// GTE is a "greater than or equal" filter. Shorthand for Greater.
func GTE(v values.Sortable) *Greater {
	return &Greater{Value: v, Equal: true}
}

var _ SortableFilter = Greater{}

type Greater struct {
	Value values.Sortable
	Equal bool
}

func (f Greater) FilterValue(v values.Value) bool {
	a, ok := v.(values.Sortable)
	if !ok && v != nil {
		return false
	}
	return f.FilterSortable(a)
}

func (f Greater) FilterSortable(v values.Sortable) bool {
	if v == nil {
		return true
	}
	c := values.Compare(v, f.Value)
	return c == +1 || (f.Equal && c == 0)
}

func (f Greater) ValuesRange() *Range {
	return &Range{Start: &f}
}

var _ SortableFilter = Range{}

// Range represents a range of sortable values.
// If inclusive is set, the range is [start, end], if not, the range is (start, end).
type Range struct {
	Start *Greater
	End   *Less
}

// isPrefix checks if the range describes a prefix. In this case Start.Value describes the prefix.
func (f Range) isPrefix() bool {
	if f.Start == nil || !f.Start.Equal {
		return false
	}

	s, ok := f.Start.Value.(values.BinaryString)
	if !ok {
		return false
	}

	end := s.PrefixEnd()
	if end == nil {
		return f.End == nil
	}

	if f.End == nil || f.End.Equal {
		return false
	}

	return values.Compare(end, f.End.Value) == 0
}

// Prefix returns a common prefix of the range. Boolean flag indicates if prefix fully describes the range.
func (f Range) Prefix() (values.BinaryString, bool) {
	if !f.isPrefix() {
		// TODO: calculate common prefix
		return nil, false
	}
	p, ok := f.Start.Value.(values.BinaryString)
	return p, ok
}

func (f Range) FilterValue(v values.Value) bool {
	a, ok := v.(values.Sortable)
	if !ok && v != nil {
		return false
	}
	return f.FilterSortable(a)
}

func (f Range) FilterSortable(v values.Sortable) bool {
	if v == nil {
		return f.Start != nil
	}
	if f.Start != nil && !f.Start.FilterSortable(v) {
		return false
	}
	if f.End != nil && !f.End.FilterSortable(v) {
		return false
	}
	return true
}

func (f Range) ValuesRange() *Range {
	return &f
}

type And []ValueFilter

func (arr And) FilterValue(v values.Value) bool {
	for _, f := range arr {
		if !f.FilterValue(v) {
			return false
		}
	}
	return true
}

type Or []ValueFilter

func (arr Or) FilterValue(v values.Value) bool {
	for _, f := range arr {
		if f.FilterValue(v) {
			return true
		}
	}
	return false
}

type Not struct {
	Filter ValueFilter
}

func (f Not) FilterValue(v values.Value) bool {
	return !f.Filter.FilterValue(v)
}

func Prefix(pref values.BinaryString) SortableFilter {
	gt := GTE(pref)
	end := pref.PrefixEnd()
	if end == nil {
		return *gt
	}
	return Range{
		Start: gt,
		End:   LT(end),
	}
}

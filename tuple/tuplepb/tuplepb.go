package tuplepb

import (
	"fmt"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/values"
)

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=. tuple.proto

// Make sure that new fields will cause compilation error.
var (
	_ tuple.Header = struct {
		Name string
		Key  []tuple.KeyField
		Data []tuple.Field
	}{}

	_ tuple.KeyField = struct {
		Name string
		Type values.SortableType
		Auto bool
	}{}

	_ tuple.Field = struct {
		Name string
		Type values.Type
	}{}
)

var (
	value2type    = make(map[values.Type]ValueType)
	type2sortable = make(map[ValueType]values.SortableType)
	type2value    = map[ValueType]values.Type{
		ValueType_TYPE_ANY:    nil,
		ValueType_TYPE_BYTES:  values.BytesType{},
		ValueType_TYPE_STRING: values.StringType{},
		ValueType_TYPE_UINT:   values.UIntType{},
		ValueType_TYPE_INT:    values.IntType{},
		ValueType_TYPE_BOOL:   values.BoolType{},
		ValueType_TYPE_TIME:   values.TimeType{},
		ValueType_TYPE_FLOAT:  values.FloatType{},
	}
)

func init() {
	for typ, v := range type2value {
		if _, ok := value2type[v]; ok {
			panic(typ.String())
		}
		value2type[v] = typ
		if v, ok := v.(values.SortableType); ok && v != nil {
			type2sortable[typ] = v
		}
	}
}

func typeOf(v values.Type) (ValueType, bool) {
	typ, ok := value2type[v]
	return typ, ok
}

func MarshalTable(t *tuple.Header) ([]byte, error) {
	table := Table{
		Name: t.Name,
		Key:  make([]KeyField, 0, len(t.Key)),
		Data: make([]Field, 0, len(t.Data)),
	}
	for _, f := range t.Key {
		tp, ok := typeOf(f.Type)
		if !ok {
			return nil, fmt.Errorf("unsupported key type: %T", f.Type)
		}
		table.Key = append(table.Key, KeyField{
			Name: f.Name, Type: tp, Auto: f.Auto,
		})
	}
	for _, f := range t.Data {
		tp, ok := typeOf(f.Type)
		if !ok {
			return nil, fmt.Errorf("unsupported value type: %T", f.Type)
		}
		table.Data = append(table.Data, Field{
			Name: f.Name, Type: tp,
		})
	}
	return table.Marshal()
}

func UnmarshalTable(p []byte) (*tuple.Header, error) {
	var t Table
	if err := t.Unmarshal(p); err != nil {
		return nil, err
	}
	table := &tuple.Header{
		Name: t.Name,
		Key:  make([]tuple.KeyField, 0, len(t.Key)),
		Data: make([]tuple.Field, 0, len(t.Data)),
	}

	for _, f := range t.Key {
		tp, ok := type2sortable[f.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported key type: %T", f.Type)
		}
		table.Key = append(table.Key, tuple.KeyField{
			Name: f.Name, Type: tp, Auto: f.Auto,
		})
	}
	for _, f := range t.Data {
		tp, ok := type2value[f.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported value type: %T", f.Type)
		}
		table.Data = append(table.Data, tuple.Field{
			Name: f.Name, Type: tp,
		})
	}
	return table, nil
}

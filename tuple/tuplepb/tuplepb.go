package tuplepb

import (
	"reflect"

	"fmt"

	"github.com/nwca/uda/tuple"
	"github.com/nwca/uda/types"
)

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=. tuple.proto

// Make sure that new fields will cause compilation error
var (
	_ tuple.Header = struct {
		Name string
		Key  []tuple.KeyField
		Data []tuple.Field
	}{}

	_ tuple.KeyField = struct {
		Name string
		Type types.Sortable
		Auto bool
	}{}

	_ tuple.Field = struct {
		Name string
		Type types.Value
	}{}
)

var (
	typBytes  types.Bytes
	typString types.String
	typUInt   types.UInt
	typInt    types.Int
	typBool   types.Bool
	typTime   types.Time
	typFloat  types.Float
)

var (
	value2type    = make(map[reflect.Type]ValueType)
	type2sortable = make(map[ValueType]types.Sortable)
	type2value    = map[ValueType]types.Value{
		ValueType_TYPE_ANY:    nil,
		ValueType_TYPE_BYTES:  &typBytes,
		ValueType_TYPE_STRING: &typString,
		ValueType_TYPE_UINT:   &typUInt,
		ValueType_TYPE_INT:    &typInt,
		ValueType_TYPE_BOOL:   &typBool,
		ValueType_TYPE_TIME:   &typTime,
		ValueType_TYPE_FLOAT:  &typFloat,
	}
)

func init() {
	for typ, v := range type2value {
		rt := reflect.TypeOf(v)
		if _, ok := value2type[rt]; ok {
			panic(typ.String())
		}
		value2type[rt] = typ
		if v, ok := v.(types.Sortable); ok && v != nil {
			type2sortable[typ] = v
		}
	}
}

func typeOf(v types.Value) (ValueType, bool) {
	typ, ok := value2type[reflect.TypeOf(v)]
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

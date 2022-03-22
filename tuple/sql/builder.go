package sqltuple

import (
	"bytes"
	"strings"
)

func (d *Dialect) NewBuilder() *Builder {
	return &Builder{d: d, buf: bytes.NewBuffer(nil)}
}

type Builder struct {
	d    *Dialect
	pi   int
	buf  *bytes.Buffer
	args []interface{}
}

func (b *Builder) Reset() {
	b.pi = 0
	b.buf.Reset()
	b.args = b.args[0:]
}

func (b *Builder) Write(s string) {
	b.buf.WriteString(s)
}

func (b *Builder) place(v interface{}) string {
	p := b.pi
	b.pi++
	b.args = append(b.args, v)
	return b.d.Placeholder(p)
}

func (b *Builder) Place(args ...interface{}) {
	if len(args) == 1 {
		if _, ok := args[0].([]interface{}); ok {
			panic("forgot to expand arguments")
		}
	}
	arr := make([]string, 0, len(args))
	for _, v := range args {
		arr = append(arr, b.place(v))
	}
	b.Write(strings.Join(arr, ", "))
}

func (b *Builder) Idents(names ...string) {
	arr := make([]string, 0, len(names))
	for _, s := range names {
		arr = append(arr, b.d.QuoteIdentifier(s))
	}
	b.Write(strings.Join(arr, ", "))
}

func (b *Builder) Literal(s string) {
	b.Write(b.d.QuoteString(s))
}

func (b *Builder) opPlace(names []string, op string, args []interface{}, sep string) {
	arr := make([]string, 0, len(names))
	for i, name := range names {
		arr = append(arr, b.d.QuoteIdentifier(name)+` `+op+` `+b.place(args[i]))
	}
	b.Write(strings.Join(arr, sep))
}

func (b *Builder) EqPlace(names []string, args []interface{}) {
	b.opPlace(names, "=", args, ", ")
}

func (b *Builder) EqPlaceAnd(names []string, args []interface{}) {
	b.Write("(")
	b.opPlace(names, "=", args, ") AND (")
	b.Write(")")
}

func (b *Builder) OpPlace(names []string, op string, args []interface{}) {
	b.opPlace(names, op, args, ", ")
}

func (b *Builder) OpPlaceAnd(names []string, op string, args []interface{}) {
	b.Write("(")
	b.opPlace(names, op, args, ") AND (")
	b.Write(")")
}

func (b *Builder) String() string {
	return b.buf.String()
}

func (b *Builder) Args() []interface{} {
	return b.args
}

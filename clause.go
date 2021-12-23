package zsql

import (
	"strconv"
	"strings"
)

type Writer interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}

// Builder builder interface
type Builder interface {
	Writer
	WriteQuoted(field interface{})
	AddVar(vars ...interface{})
}

// Clause
type Clause struct {
	Name       string // WHERE
	Expression Expression
}

func (c Clause) Build(builder Builder) {
	if c.Expression != nil {
		if c.Name != "" {
			builder.WriteString(c.Name)
			builder.WriteByte(' ')
		}

		c.Expression.Build(builder)
	}
}

type IClause interface {
	Name() string
	Build(Builder)
	MergeClause(*Clause)
}

type orderByColumn struct {
	Column string
	Desc   bool
}

type orderBy struct {
	Columns []orderByColumn
}

func (orderBy orderBy) Name() string {
	return "ORDER BY"
}

func (orderBy orderBy) Build(builder Builder) {
	for idx, column := range orderBy.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}

		builder.WriteQuoted(column.Column)
		if column.Desc {
			builder.WriteString(" DESC")
		}
	}
}

func (ob orderBy) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(orderBy); ok {
		copiedColumns := make([]orderByColumn, len(v.Columns))
		copy(copiedColumns, v.Columns)
		ob.Columns = append(ob.Columns, copiedColumns...)
	}

	clause.Expression = ob
}

type groupByColumn struct {
	Column string
}

type groupBy struct {
	Columns []groupByColumn
}

func (groupBy groupBy) Name() string {
	return "GROUP BY"
}

func (groupBy groupBy) Build(builder Builder) {
	for idx, column := range groupBy.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}

		builder.WriteQuoted(column.Column)
	}
}

func (gb groupBy) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(groupBy); ok {
		copiedColumns := make([]groupByColumn, len(v.Columns))
		copy(copiedColumns, v.Columns)
		gb.Columns = append(gb.Columns, copiedColumns...)
	}

	clause.Expression = gb
}

type Limit struct {
	Limit  int
	Offset int
}

func (limit Limit) Name() string {
	return "LIMIT"
}

func (limit Limit) Build(builder Builder) {
	if limit.Limit > 0 {
		builder.WriteString("LIMIT ")
		builder.WriteString(strconv.Itoa(limit.Limit))
	}
	if limit.Offset > 0 {
		if limit.Limit > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString("OFFSET ")
		builder.WriteString(strconv.Itoa(limit.Offset))
	}
}

func (limit Limit) MergeClause(clause *Clause) {
	clause.Name = ""

	if v, ok := clause.Expression.(Limit); ok {
		if limit.Limit == 0 && v.Limit != 0 {
			limit.Limit = v.Limit
		}
		if limit.Offset == 0 && v.Offset > 0 {
			limit.Offset = v.Offset
		} else if limit.Offset < 0 {
			limit.Offset = 0
		}
	}
	clause.Expression = limit
}

type WhereEquality string

const (
	_ WhereEquality = ""
	// equal
	WE_EQ WhereEquality = "="
	// not equal
	WE_NE WhereEquality = "<>"
	// like
	WE_LK WhereEquality = "LIKE"
	// less than
	WE_LT WhereEquality = "<"
	// less than or equal to
	WE_LTE WhereEquality = "<="
	// greater than
	WE_GT WhereEquality = ">"
	// greater than or equal to
	WE_GTE     WhereEquality = ">="
	WE_BETWEEN WhereEquality = "BETWEEN"
	WE_IN      WhereEquality = "IN"
)

func (we WhereEquality) Equality() string {
	return string(we)
}

func ToWhereEquality(op string) WhereEquality {
	switch strings.ToUpper(op) {
	case "=":
		return WE_EQ
	case "<>":
		return WE_NE
	case "LIKE":
		return WE_LK
	case "<":
		return WE_LT
	case "<=":
		return WE_LTE
	case ">":
		return WE_GT
	case ">=":
		return WE_GTE
	case "BETWEEN":
		return WE_BETWEEN
	case "IN":
		return WE_IN
	default:
		return ""
	}
}

type Where struct {
	Columns []WhereColumn
}

func (where Where) Name() string {
	return "WHERE"
}

func (where Where) Build(builder Builder) {
	if len(where.Columns) == 0 {
		return
	}
	if len(where.Columns) == 1 && where.Columns[0].Len() > 0 {
		where.Columns = where.Columns[0].columns()
	}
	for idx, column := range where.Columns {
		if idx > 0 {
			builder.WriteString(" " + column.name() + " ")
		}
		column.build(builder)
	}
}

func (where Where) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Where); ok {
		copiedColumns := make([]WhereColumn, len(v.Columns))
		copy(copiedColumns, v.Columns)
		for _, column := range copiedColumns {
			if column.IsEmpty() {
				panic("has empty WhereColumn")
				continue
			}
			where.Columns = append(where.Columns, column)
		}

	}
	clause.Expression = where
}

func (where *Where) And(field string, quality WhereEquality, value interface{}) WhereColumn {
	if quality.Equality() == "" {
		panic("where builder and unsupported quality " + quality.Equality())
	}
	if quality == WE_IN || quality == WE_BETWEEN {
		if value == nil {
			panic("where builder for quanlity in and between received empty value")
		}
	}
	and := &and{
		Field:    field,
		Equality: quality,
		Value:    value,
	}
	where.Columns = append(where.Columns, and)
	return and
}

func And(field string, quality, value interface{}) WhereColumn {
	if field == "" {
		return &and{Field: ""}
	}
	switch v := quality.(type) {
	case WhereEquality:
		return &and{Field: field, Equality: v, Value: value}
	case string:
		s := WhereEquality(v)
		if s.Equality() != "" {
			return &and{Field: field, Equality: s, Value: value}
		}
		ts := ToWhereEquality(v)
		if ts != "" {
			return &and{Field: field, Equality: ts, Value: value}
		}
	}
	return &and{Field: ""}
}

func (where *Where) Or(field string, quality WhereEquality, value interface{}) WhereColumn {
	if quality.Equality() == "" {
		panic("where builder and unsupported quality " + quality.Equality())
	}
	if quality == WE_IN || quality == WE_BETWEEN {
		if value == nil {
			panic("where builder for quanlity in and between received empty value")
		}
	}
	or := &or{
		Field:    field,
		Equality: quality,
		Value:    value,
	}
	where.Columns = append(where.Columns, or)
	return or
}

type WhereColumn interface {
	name() string
	C(WhereColumn)
	Len() int
	build(Builder)
	columns() []WhereColumn
	IsEmpty() bool
}

type and struct {
	Columns  []WhereColumn
	Field    string
	Equality WhereEquality
	Semantic string
	Value    interface{}
}

func (wc and) IsEmpty() bool {
	return wc.Len() == 0 && wc.Field == "" && wc.Semantic == ""
}

func (wc and) name() string {
	return "AND"
}

func (wc *and) C(sub WhereColumn) {
	if wc.Columns == nil {
		wc.Columns = []WhereColumn{
			&and{
				Field: wc.Field,
				Value: wc.Value,
			},
		}
	}
	wc.Columns = append(wc.Columns, sub)
}

func (wc and) Len() int {
	return len(wc.Columns)
}

func (wc and) columns() []WhereColumn {
	return wc.Columns
}

func (wc and) build(builder Builder) {
	if len(wc.Columns) > 1 {
		builder.WriteString("(")
		for idx, column := range wc.Columns {
			if idx > 0 {
				builder.WriteString(" " + column.name() + " ")
			}
			column.build(builder)
		}
		builder.WriteString(")")
		return
	}
	if len(wc.Columns) == 1 {
		wc.Columns[0].build(builder)
		return
	}
	we := wc.Equality.Equality()
	if we == "" {
		if wc.Semantic == "" {
			panic("equlity not support and sematic is empty")
		}
		builder.WriteString(" " + wc.Semantic)
		vs := wc.Value.([]interface{})
		builder.AddVar(vs...)
		return
	}
	switch wc.Equality {
	case WE_BETWEEN:
		builder.WriteQuoted(wc.Field)
		builder.WriteString(" " + we + " ? AND ?")
		vs := wc.Value.([]interface{})
		builder.AddVar(vs[0])
		builder.AddVar(vs[1])
	case WE_IN:
		vs := wc.Value.([]interface{})
		builder.WriteQuoted(wc.Field)
		inClause := strings.Repeat("?,", len(vs))
		builder.WriteString(" " + we + "(" + inClause[:len(inClause)-1] + ")")
		builder.AddVar(vs...)
	default:
		builder.WriteQuoted(wc.Field)
		builder.WriteString(" " + we + " ?")
		builder.AddVar(wc.Value)
	}

}

type or struct {
	Columns  []WhereColumn
	Field    string
	Equality WhereEquality
	Semantic string
	Value    interface{}
}

func (wc or) name() string {
	return "OR"
}

func (wc or) IsEmpty() bool {
	return wc.Len() == 0 && wc.Field == "" && wc.Semantic == ""
}

func (wc *or) C(sub WhereColumn) {
	if wc.Columns == nil {
		wc.Columns = []WhereColumn{
			&or{
				Field: wc.Field,
				Value: wc.Value,
			},
		}
	}
	wc.Columns = append(wc.Columns, sub)
}

func (wc or) Len() int {
	return len(wc.Columns)
}

func (wc or) build(builder Builder) {
	if len(wc.Columns) > 1 {
		builder.WriteString("(")
		for idx, column := range wc.Columns {
			if idx > 0 {
				builder.WriteString(" " + column.name() + " ")
			}
			column.build(builder)
		}
		builder.WriteString(")")
		return
	}
	if len(wc.Columns) == 1 {
		wc.Columns[0].build(builder)
		return
	}
	we := wc.Equality.Equality()
	if we == "" {
		if wc.Semantic == "" {
			panic("equlity not support and sematic is empty")
			return
		}
		builder.WriteString(" " + wc.Semantic)
		vs := wc.Value.([]interface{})
		builder.AddVar(vs...)
		return
	}
	switch wc.Equality {
	case WE_BETWEEN:
		builder.WriteQuoted(wc.Field)
		builder.WriteString(" " + we + " ? AND ?")
		vs := wc.Value.([]interface{})
		builder.AddVar(vs[0])
		builder.AddVar(vs[1])
	case WE_IN:
		vs := wc.Value.([]interface{})
		builder.WriteQuoted(wc.Field)

		inClause := strings.Repeat("?,", len(vs))
		builder.WriteString(" " + we + "(" + inClause[:len(inClause)-1] + ")")
		builder.AddVar(vs...)
	default:
		builder.WriteQuoted(wc.Field)
		builder.WriteString(" " + we + " ?")
		builder.AddVar(wc.Value)
	}

}

func (wc or) columns() []WhereColumn {
	return wc.Columns
}

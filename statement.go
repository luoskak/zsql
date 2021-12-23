package zsql

import (
	"context"
	"reflect"
	"strings"

	"github.com/luoskak/zsql/pkg/schema"
)

var (
// _ Builder = Statement{}
)

type Statement struct {
	Context context.Context
	*DB
	Schema       *schema.Schema
	SQL          strings.Builder
	Vals         []interface{}
	Model        interface{}
	Dest         interface{}
	ConnPool     ConnPool
	Clauses      map[string]Clause
	NameMapper   map[string]string
	BuildClauses []string
	ReflectValue reflect.Value
	Table        string
}

func (st *Statement) clone() *Statement {
	newStmt := &Statement{
		Schema:     st.Schema,
		Clauses:    make(map[string]Clause),
		NameMapper: make(map[string]string),
	}
	if st.SQL.Len() > 0 {
		newStmt.SQL.WriteString(st.SQL.String())
	}
	return newStmt
}

func (st *Statement) Parse(value interface{}) (err error) {
	if st.Schema, err = schema.Parse(value, st.DB.opts.cacheStore, st.DB.opts.namingStrategy); err == nil && st.Table == "" {
		st.Table = st.Schema.Table
	}
	return err
}

func (st *Statement) WriteString(str string) (int, error) {
	return st.SQL.WriteString(str)
}

func (st *Statement) WriteByte(c byte) error {
	return st.SQL.WriteByte(c)
}

func (st *Statement) WriteQuoted(value interface{}) {
	switch v := value.(type) {
	case string:
		if nn, has := st.NameMapper[v]; has {
			st.WriteString(nn)
			return
		}
		st.WriteString(v)
	}
}

func (st *Statement) AddVar(vars ...interface{}) {
	for _, val := range vars {
		switch v := val.(type) {
		// 过滤不合法
		case map[string]interface{}:
		default:
			st.Vals = append(st.Vals, v)
		}
	}

}

func (st *Statement) AddClause(v IClause) {
	name := v.Name()
	c := st.Clauses[name]
	c.Name = name
	v.MergeClause(&c)
	st.Clauses[name] = c
}

func (st *Statement) Build(clauses ...string) {

	for _, name := range clauses {
		if c, ok := st.Clauses[name]; ok {
			st.WriteString(" ")
			c.Build(st)
		}
	}
}

func (st *Statement) Reset() (tx *DB) {
	tx = st.getInstance()
	tx.Statement.Model = nil
	tx.Statement.BuildClauses = nil
	tx.Statement.Clauses = make(map[string]Clause)
	tx.Statement.NameMapper = make(map[string]string)
	tx.Error = nil
	// if _, ok := tx.Statement.ConnPool.(TxCommitter); !ok {
	// 	tx.Statement.ConnPool = nil
	// }
	return
}

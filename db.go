package zsql

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/luoskak/logger"
	"github.com/luoskak/zsql/pkg/parser"
	"github.com/luoskak/zsql/pkg/schema"
)

type DB struct {
	rConn         ConnPool
	wConn         ConnPool
	log           *logger.Logger
	opts          *mwOptions
	Statement     *Statement
	RowsAffected  int64
	LastInsertId  int64
	Error         error
	clone         int
	clausesCaller func(operation string) []string
	listener      *pgxListener
}

func (m *DB) TypeName() string {
	return "mysql"
}

func (m *DB) ID() string {
	return ""
}

func (db *DB) getInstance() *DB {
	if db.clone > 0 {
		tx := &DB{
			rConn:         db.rConn,
			wConn:         db.wConn,
			opts:          db.opts,
			Error:         db.Error,
			log:           db.log,
			clausesCaller: db.clausesCaller,
			listener:      db.listener,
		}

		if db.clone == 1 {
			tx.Statement = &Statement{
				Context:    context.Background(),
				DB:         tx,
				Clauses:    make(map[string]Clause),
				NameMapper: make(map[string]string),
			}
		} else {
			tx.Statement = db.Statement.clone()
			tx.Statement.DB = tx
		}

		return tx

	}
	return db
}

// MustWrite 当使用Query时可以强制用读库
func (db *DB) MustWrite() (tx *DB) {
	tx = db.getInstance()
	if _, ok := tx.Statement.ConnPool.(TxCommitter); !ok {
		tx.Statement.ConnPool = db.wConn
	}
	return
}

func (db *DB) Query(sql string, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.SQL.WriteString(sql)
	tx.Statement.Vals = args
	tx.Statement.BuildClauses = tx.clausesCaller("SELECT")
	if tx.Statement.ConnPool == nil {
		tx.Statement.ConnPool = db.rConn
	}
	return
}

func (db *DB) QueryReturn(sql string, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.MustWrite()
	return tx.Query(sql, args...)
}

func (db *DB) Exec(sql string, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.SQL.WriteString(sql)
	tx.Statement.Vals = args
	if tx.Statement.ConnPool == nil {
		tx.Statement.ConnPool = db.wConn
	}
	return executeExec(tx)
}

func (db *DB) Reset() (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Reset()
	return
}

func (db *DB) AddNameMap(old, new string) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.NameMapper[old] = new
	return tx
}

func (db *DB) Where(where ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(where) == 0 {
		return
	}
	switch v := where[0].(type) {
	case map[string]interface{}:
		var travelWhere func(where map[string]interface{}) []WhereColumn
		travelWhere = func(where map[string]interface{}) []WhereColumn {
			var columns []WhereColumn
			for field, v := range where {
				vm, ok := v.(map[string]interface{})
				if !ok {
					// 不符合要求的
					continue
				}
				for op, v := range vm {
					if op == "OR" || op == "AND" {
						if sm, is := v.(map[string]interface{}); is {
							cs := travelWhere(sm)
							if op == "OR" {
								columns = append(columns, &or{Columns: cs})
							} else {
								columns = append(columns, &and{Columns: cs})
							}
						}
						continue
					}
					if op == "BETWEEN" {
						if vs, is := v.([]interface{}); is && len(vs) == 2 {
							columns = append(columns, &and{Field: field, Equality: WE_BETWEEN, Value: vs})
						}
						continue
					}
					if op == "IN" {
						if vs, is := v.([]interface{}); is && len(vs) > 0 {
							columns = append(columns, &and{Field: field, Equality: WE_IN, Value: vs})
						}
						continue
					}
					quality := ToWhereEquality(op)
					if quality == "" {
						continue
					}
					columns = append(columns, &and{
						Field:    field,
						Equality: quality,
						Value:    v,
					})
				}
			}
			return columns
		}
		columns := travelWhere(v)
		if columns != nil {
			tx.Statement.AddClause(Where{Columns: columns})
		}
	case WhereColumn:
		tx.Statement.AddClause(Where{Columns: []WhereColumn{v}})
	default:
		argLen := len(where)
		if argLen == 3 {
			if _, ok := where[0].(string); ok {
				if w1, ok := where[1].(string); ok {
					equality := ToWhereEquality(w1)
					if equality == WE_IN || equality == WE_BETWEEN {
						vs, ok := where[2].([]interface{})
						if !ok {
							return
						}
						if len(vs) == 0 {
							return
						}
						if len(vs) != 2 && equality == WE_BETWEEN {
							return
						}
					}
					tx.Statement.AddClause(Where{Columns: []WhereColumn{
						&and{
							Field:    where[0].(string),
							Equality: equality,
							Value:    where[2],
						},
					}})
					return
				}
				if w1, ok := where[1].(WhereEquality); ok {
					if w1 == WE_IN || w1 == WE_BETWEEN {
						vs, ok := where[2].([]interface{})
						if !ok {
							return
						}
						if len(vs) == 0 {
							return
						}
						if len(vs) != 2 && w1 == WE_BETWEEN {
							return
						}
					}
					tx.Statement.AddClause(Where{Columns: []WhereColumn{
						&and{
							Field:    where[0].(string),
							Equality: w1,
							Value:    where[2],
						},
					}})
					return
				}
			}
		}
		if _, ok := where[0].(string); ok {
			var vs []interface{}
			for i, v := range where {
				if i == 0 {
					continue
				}
				vs = append(vs, v)
			}
			tx.Statement.AddClause(Where{Columns: []WhereColumn{
				&and{
					Semantic: where[0].(string),
					Value:    vs,
				},
			}})
		}
	}
	return
}

func (db *DB) Order(value interface{}) (tx *DB) {
	tx = db.getInstance()

	switch v := value.(type) {
	case orderByColumn:
		tx.Statement.AddClause(orderBy{
			Columns: []orderByColumn{v},
		})
	case string:
		if v != "" {
			tx.Statement.AddClause(orderBy{
				Columns: []orderByColumn{
					{Column: v},
				},
			})
		}
	case [][]string:
		if len(v) == 0 {
			return
		}
		var columns []orderByColumn
		for _, i := range v {
			switch i[1] {
			case "ASC":
				columns = append(columns, orderByColumn{Column: i[0]})
			case "DESC":
				columns = append(columns, orderByColumn{Column: i[0], Desc: true})
			}
		}
		tx.Statement.AddClause(orderBy{Columns: columns})
	}
	return
}

func (db *DB) GroupBy(column string) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.AddClause(groupBy{Columns: []groupByColumn{{Column: column}}})
	return
}

func (db *DB) Limit(limit int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.AddClause(Limit{Limit: limit})
	return
}

func (db *DB) Offset(offset int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.AddClause(Limit{Offset: offset})
	return
}

func (db *DB) Paging(page, perPage int) (tx *DB) {
	if page < 1 {
		page = 1
	}
	tx = db.getInstance()
	tx.Statement.AddClause(Limit{Offset: (page - 1) * perPage, Limit: perPage})
	return
}

func (db *DB) Find(dest interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Dest = dest
	return executeQuery(tx)
}

func (db *DB) AddError(err error) error {
	if db.Error == nil {
		db.Error = err
	} else if err != nil {
		db.Error = fmt.Errorf("%v: %w", db.Error, err)
	}
	return db.Error
}

func executeQuery(db *DB) *DB {
	st := db.Statement

	if st.Model == nil {
		st.Model = st.Dest
	} else if st.Dest == nil {
		st.Dest = st.Model
	}

	if st.Model != nil {
		if err := st.Parse(st.Model); err != nil && (!errors.Is(err, schema.ErrUnsupportedDataType) || (st.Table == "" && st.SQL.Len() == 0)) {
			if errors.Is(err, schema.ErrUnsupportedDataType) && st.Table == "" {
				db.AddError(fmt.Errorf("%w: Table not set", err))
			} else {
				db.AddError(err)
			}
		}
	}

	if st.Dest != nil {
		st.ReflectValue = reflect.ValueOf(st.Dest)
		for st.ReflectValue.Kind() == reflect.Ptr {
			if st.ReflectValue.IsNil() && st.ReflectValue.CanAddr() {
				st.ReflectValue.Set(reflect.New(st.ReflectValue.Type().Elem()))
			}

			st.ReflectValue = st.ReflectValue.Elem()
		}
		if !st.ReflectValue.IsValid() {
			db.AddError(ErrInvalidValue)
		}
	}

	if db.Error == nil {
		st.Build(st.BuildClauses...)
		sql := st.SQL.String()
		db.log.Info(parser.ExplainSQL(sql, nil, "'", st.Vals...))
		rows, err := st.ConnPool.QueryContext(st.Context, sql, st.Vals...)
		if err != nil {
			db.AddError(err)
			return db
		}
		defer rows.Close()

		Scan(rows, db)
	}

	st.SQL.Reset()
	st.Vals = nil

	return db
}

func executeExec(db *DB) *DB {
	st := db.Statement
	if db.Error == nil {
		st.Build(st.BuildClauses...)
		sql := st.SQL.String()
		db.log.Info(parser.ExplainSQL(sql, nil, "'", st.Vals...))
		result, err := st.ConnPool.ExecContext(st.Context, sql, st.Vals...)
		if err != nil {
			db.AddError(err)
			return db
		}
		affected, err := result.RowsAffected()
		if err != nil {
			db.AddError(err)
			return db
		}
		st.RowsAffected = affected
		// postgres driver not support this
		// last, err := result.LastInsertId()
		// if err != nil {
		// 	db.AddError(err)
		// 	return db
		// }
		// st.LastInsertId = last
	}
	st.SQL.Reset()
	st.Vals = nil

	return db
}

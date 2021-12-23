package zsql

import (
	"database/sql"
	"reflect"
)

func (db *DB) Begin(opts ...*sql.TxOptions) *DB {
	var (
		tx  = db.getInstance()
		opt *sql.TxOptions
		err error
	)

	if len(opts) > 0 {
		opt = opts[0]
	}
	if tx.Statement.ConnPool == nil {
		tx.Statement.ConnPool = tx.wConn
	}

	if beginner, ok := tx.Statement.ConnPool.(TxBeginner); ok {
		tx.Statement.ConnPool, err = beginner.BeginTx(tx.Statement.Context, opt)
	} else {
		err = ErrInvalidTransaction
	}

	if err != nil {
		tx.AddError(err)
	}
	return tx

}

func (db *DB) Commit() *DB {
	if committer, ok := db.Statement.ConnPool.(TxCommitter); ok && committer != nil && !reflect.ValueOf(committer).IsNil() {
		db.AddError(committer.Commit())
		db.Statement.ConnPool = nil
	} else {
		db.AddError(ErrInvalidTransaction)
	}
	return db
}

func (db *DB) Rollback() *DB {
	if committer, ok := db.Statement.ConnPool.(TxCommitter); ok && committer != nil {
		if !reflect.ValueOf(committer).IsNil() {
			db.AddError(committer.Rollback())
			db.Statement.ConnPool = nil
		}
	} else {
		db.AddError(ErrInvalidTransaction)
	}
	return db
}

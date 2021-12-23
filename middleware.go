package zsql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/luoskak/logger"
	"github.com/luoskak/mist"
)

const MiddlewareName = "zsql"

func init() {
	mist.DefaultManager.Register(MiddlewareName, &Middleware{
		dbs: make(map[string]*DB),
	})
}

type dbKey struct {
}

type Middleware struct {
	opts *mwOptions
	dbs  map[string]*DB
}

func (m *Middleware) Inter(full bool) mist.Interceptor {
	return func(ctx context.Context, req interface{}, info *mist.ServerInfo, handler mist.Handler) (interface{}, error) {
		// standard
		// TODO： 注入某些数据库的使用权限
		return handler(context.WithValue(ctx, dbKey{}, m), req)
	}
}

func (m *Middleware) Init(opt []mist.Option) {
	opts := defaultMwOptions
	for _, o := range opt {
		o.Apply(&opts)
	}
	if opts.cacheStore == nil {
		opts.cacheStore = &sync.Map{}
	}

	if len(opts.dbOpts) == 0 {
		panic("has no addressed mysql")
	}

	var errs error
	for _, dbOpt := range opts.dbOpts {
		dbName := dbOpt.name
		db := &DB{
			opts:          &opts,
			clausesCaller: opts.clauseCaller(dbOpt.driverName),
		}
		db.log = logger.NewLogger("Middleware:%s->%s", MiddlewareName, dbName)
		// TODO: 将driver分离
		switch dbOpt.driverName {
		case "mysql":
			if rConn, err := sql.Open(dbOpt.driverName, dbOpt.readAddress); err != nil {
				errs = fmt.Errorf("%v; %s read got %w", errs, dbName, err)
			} else {

				rConn.SetMaxIdleConns(opts.maxIdleCound)
				rConn.SetMaxOpenConns(opts.maxOpenCound)
				rConn.SetConnMaxLifetime(opts.maxLifeTime)
				db.rConn = rConn
			}

			if wConn, err := sql.Open(dbOpt.driverName, dbOpt.writeAddress); err != nil {
				errs = fmt.Errorf("%v; %s write got %w", errs, dbName, err)
			} else {

				wConn.SetMaxIdleConns(opts.maxIdleCound)
				wConn.SetMaxOpenConns(opts.maxOpenCound)
				wConn.SetConnMaxLifetime(opts.maxLifeTime)
				db.wConn = wConn
			}
			m.dbs[dbName] = db

			db.Statement = &Statement{
				DB: db,
			}
			db.clone = 1
		case "postgres":
			rc, err := pgx.ParseConfig(dbOpt.readAddress)
			if err != nil {
				errs = fmt.Errorf("%v; %s read got %w", errs, dbName, err)
			} else {
				db.rConn = stdlib.OpenDB(*rc)
				db.listener = &pgxListener{
					openFunc: func(ctx context.Context) (*pgx.Conn, error) {
						return pgx.Connect(ctx, dbOpt.readAddress)
					},
				}
			}
			wc, err := pgx.ParseConfig(dbOpt.writeAddress)
			if err != nil {
				errs = fmt.Errorf("%v; %s write got %w", errs, dbName, err)
			} else {
				db.wConn = stdlib.OpenDB(*wc)
			}

			m.dbs[dbName] = db
			db.Statement = &Statement{
				DB: db,
			}
			db.clone = 1
		}

	}
	if errs != nil {
		panic(errs)
	}
	if _, hasDefault := m.dbs["default"]; !hasDefault {
		m.dbs["default"] = m.dbs[opts.dbOpts[0].name]
	}
	m.opts = &opts

}

func (m *Middleware) Close() error {
	var errs error
	for n, db := range m.dbs {
		if poolConn, ok := db.rConn.(io.Closer); ok {
			if err := poolConn.Close(); err != nil {
				errs = fmt.Errorf("%v; %s read close %w", errs, n, err)
			}
		}
		if poolConn, ok := db.wConn.(io.Closer); ok {
			if err := poolConn.Close(); err != nil {
				errs = fmt.Errorf("%v; %s write close %w", errs, n, err)
			}
		}

	}

	return errs
}

package zsql

import (
	"strings"
	"sync"
	"time"

	"github.com/luoskak/mist"
	"github.com/luoskak/zsql/pkg/schema"
)

type mwOptions struct {
	dbOpts         []*dbOptions
	maxIdleCound   int
	maxOpenCound   int
	maxLifeTime    time.Duration
	namingStrategy schema.Namer
	cacheStore     *sync.Map
	clauseCaller   func(driverName string) func(opertion string) []string
}

var defaultMwOptions = mwOptions{
	maxIdleCound:   500,
	maxOpenCound:   50,
	maxLifeTime:    time.Hour,
	namingStrategy: schema.NamingStrategy{},
	clauseCaller:   clausesDefaultCaller,
}

type dbOptions struct {
	name, driverName, readAddress, writeAddress string
}

func MaxIdle(count int) mist.Option {
	return mist.NewFuncMyOption(MiddlewareName, func(i mist.Options) {
		opts := i.(*mwOptions)
		if count > 0 {
			opts.maxIdleCound = count
		}
	})
}

func MaxOpen(count int) mist.Option {
	return mist.NewFuncMyOption(MiddlewareName, func(i mist.Options) {
		opts := i.(*mwOptions)
		if count > 0 {
			opts.maxOpenCound = count
		}
	})
}

// name will set to be default when empty
func MysqlAddress(name, read, write string) mist.Option {
	if read == "" || write == "" {
		panic("read or write can not be empty")
	}
	return mist.NewFuncMyOption(MiddlewareName, func(i mist.Options) {
		opts := i.(*mwOptions)
		for _, dbOp := range opts.dbOpts {
			if dbOp.name == name {
				panic("same name sql address")
			}
		}
		if name == "" {
			name = "default"
		}
		// 拒绝只读数据库
		write = strings.ReplaceAll(write, "&rejectReadOnly=true", "") + "&rejectReadOnly=true"
		dbOp := &dbOptions{
			name:         name,
			driverName:   "mysql",
			readAddress:  read,
			writeAddress: write,
		}
		opts.dbOpts = append(opts.dbOpts, dbOp)
	})
}

func PgAddress(name, read, write string) mist.Option {
	if read == "" || write == "" {
		panic("read or write can not be empty")
	}
	return mist.NewFuncMyOption(MiddlewareName, func(i mist.Options) {
		opts := i.(*mwOptions)
		for _, dbOp := range opts.dbOpts {
			if dbOp.name == name {
				panic("same name sql address")
			}
		}
		if name == "" {
			name = "default"
		}
		dbOp := &dbOptions{
			name:         name,
			driverName:   "postgres",
			readAddress:  read,
			writeAddress: write,
		}
		opts.dbOpts = append(opts.dbOpts, dbOp)
	})
}

func clausesDefaultCaller(driver string) func(operation string) []string {
	switch driver {
	case "mysql":
		return func(operation string) []string {
			switch operation {
			case "SELECT":
				return []string{
					"WHERE",
					"GROUP BY",
					"ORDER BY",
					"LIMIT",
				}
			default:
				return nil
			}
		}
	case "postgres":
		return func(operation string) []string {
			switch operation {
			case "SELECT":
				return []string{
					"WHERE",
					"GROUP BY",
					"ORDER BY",
					"LIMIT",
				}
			default:
				return nil
			}
		}
	}
	return nil
}

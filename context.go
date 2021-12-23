package zsql

import (
	"context"
)

func fromIncommingContext(ctx context.Context) *Middleware {
	mw := ctx.Value(dbKey{})
	if mw == nil {
		panic("[middleware] use invisible mysql")
	}
	d, ok := mw.(*Middleware)
	if !ok {
		panic("[middleware] mysql value error")
	}
	return d
}

func Get(ctx context.Context, args ...string) *DB {
	mw := fromIncommingContext(ctx)
	if len(args) == 1 && args[0] != "" {
		db, had := mw.dbs[args[0]]
		if !had {
			panic("the name " + args[0] + " db was not exist")
		}
		return db
	}
	return mw.dbs["default"]
}

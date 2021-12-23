package zsql

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v4"
	"github.com/luoskak/plant/pkg/rxjs"
	"github.com/luoskak/plant/pkg/rxjs/abstract"
)

var (
	ErrInvalidConnForListen = errors.New("conn does not support LISTEN / NOTIFY")
)

// listener support by pgx on postgres
type pgxListener struct {
	openFunc func(ctx context.Context) (*pgx.Conn, error)
}

func (db *DB) Listen(ctx context.Context, channel string) (abstract.Observable, error) {
	tx := db.getInstance()
	if tx.listener == nil {
		return nil, ErrInvalidConnForListen
	}
	return rxjs.Observable(func(observer rxjs.Observer) {
		conn, err := db.listener.openFunc(ctx)
		if err != nil {
			observer.Err(err)
			return
		}
		defer func() {
			err := conn.Close(ctx)
			if err != nil {
				observer.Err(err)
				return
			}
		}()
		if conn.PgConn().ParameterStatus("crdb_version") != "" {
			observer.Err(ErrInvalidConnForListen)
			return
		}
		_, err = conn.Exec(ctx, "listen "+channel)
		if err != nil {
			observer.Err(err)
			return
		}
		for {
			notification, err := conn.WaitForNotification(ctx)
			if err != nil {
				observer.Err(err)
				return
			}
			observer.Next(notification.Payload)
		}
	}), nil
}

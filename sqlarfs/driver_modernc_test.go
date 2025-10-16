//go:build !cgo || modernc

package sqlarfs_test

import (
	_ "modernc.org/sqlite"
)

func init() {
	// go test -v -ldflags="-X github.com/dolmen-go/sqlar/sqlarfs_test.sqliteDriver=sqlite" -tags=modernc
	if sqliteDriver == "" {
		sqliteDriver = "sqlite"
	}
}

//go:build !sqlite.no.modernc && (sqlite.modernc || (!cgo && !sqlite.ncruces))

package sqlarfs_test

import (
	_ "modernc.org/sqlite"
)

func init() {
	// go test -v -ldflags="-X github.com/dolmen-go/sqlar/sqlarfs_test.sqliteDriver=sqlite" -tags=sqlite.modernc
	if sqliteDriver == "" {
		sqliteDriver = "sqlite"
	}
}

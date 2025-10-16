//go:build sqlite.ncruces

package sqlarfs_test

import (
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func init() {
	// go test -v -ldflags="-X github.com/ncruces/go-sqlite3/driver.driverName=sqlite.ncruces -X github.com/dolmen-go/sqlar/sqlarfs_test.sqliteDriver=sqlite.ncruces" -tags=sqlite.ncruces
	if sqliteDriver == "" {
		sqliteDriver = "sqlite3"
	}
}

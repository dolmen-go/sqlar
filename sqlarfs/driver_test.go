//go:build cgo && !modernc

package sqlarfs_test

import _ "github.com/mattn/go-sqlite3"

func init() {
	// See https://github.com/mattn/go-sqlite3/blob/master/sqlite3.go#L237
	// go test -v -ldflags="-X github.com/mattn/go-sqlite3.driverName=toto -X github.com/dolmen-go/sqlar/sqlarfs_test.sqliteDriver=toto" -run 'TestShowDriver$'

	if sqliteDriver == "" {
		sqliteDriver = "sqlite3"
	}
}

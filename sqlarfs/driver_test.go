//go:build cgo && !modernc

package sqlarfs_test

import _ "github.com/mattn/go-sqlite3"

func init() {
	sqliteDriver = "sqlite3"
}

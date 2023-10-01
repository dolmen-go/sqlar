//go:build !cgo || modernc

package sqlarfs_test

import (
	_ "modernc.org/sqlite"
)

func init() {
	sqliteDriver = "sqlite"
}

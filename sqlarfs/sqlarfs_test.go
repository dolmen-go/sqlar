package sqlarfs_test

import (
	"database/sql"
	"io/fs"
	"testing"

	"github.com/dolmen-go/sqlar/sqlarfs"
	_ "github.com/mattn/go-sqlite3"
)

var sqliteDriver string

func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(sqliteDriver, path)
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}

	t.Cleanup(func() {
		err := db.Close()
		if err != nil {
			t.Error("close archive DB:", err)
		}
	})

	return db
}

func openFS(t *testing.T, path string) fs.FS {
	db := openDB(t, path)
	return sqlarfs.New(db)
}

func TestSimple(t *testing.T) {
	ar := openFS(t, "testdata/simple.sqlar")
	// ar := os.DirFS("testdata/simple")
	fs.WalkDir(ar, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Errorf("%q: %v", path, err)
			return nil
		}
		info, _ := d.Info()
		t.Logf("%24q: %s %11o %10d %-30s  %s", path, info.Mode(), uint32(info.Mode()), info.Size(), info.ModTime(), info.Name())
		return nil
	})
}

func TestDir(t *testing.T) {
	ar := openFS(t, "testdata/dir.sqlar")
	// ar := os.DirFS("testdata/dir")
	fs.WalkDir(ar, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Errorf("%q: %v", path, err)
			return nil
		}
		info, _ := d.Info()
		t.Logf("%24q: %s %11o %10d %-30s  %s", path, info.Mode(), uint32(info.Mode()), info.Size(), info.ModTime(), info.Name())
		return nil
	})
}

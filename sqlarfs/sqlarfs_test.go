package sqlarfs_test

import (
	"database/sql"
	"io"
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

func openFS(t *testing.T, path string, opts ...sqlarfs.Option) fs.FS {
	db := openDB(t, path)
	return sqlarfs.New(db, opts...)
}

func TestSimple(t *testing.T) {
	ar := openFS(t, "testdata/simple.sqlar", sqlarfs.PermOwner)
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

	for _, name := range []string{
		".",
		"foo.txt",
		"bar.txt",
	} {
		fi, err := fs.Stat(ar, name)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		t.Log(fi)
		if fi.IsDir() {
			continue
		}

		f, err := ar.Open(name)
		if err != nil {
			t.Errorf("Open(%q): %v", name, err)
			continue
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			t.Errorf("Read(%q): %v", name, err)
		}
		t.Logf("%s: %q", name, string(b))
	}
}

func TestDir(t *testing.T) {
	ar := openFS(t, "testdata/dir.sqlar", sqlarfs.PermOwner)
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

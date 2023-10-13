package sqlarfs_test

import (
	"database/sql"
	"io"
	"io/fs"
	"runtime"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/dolmen-go/sqlar/sqlarfs"
	_ "github.com/mattn/go-sqlite3"
)

var sqliteDriver string

func TestShowDriver(t *testing.T) {
	t.Log(sqliteDriver)
}

func openDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	db, err := sql.Open(sqliteDriver, path)
	if err != nil {
		tb.Fatalf("open %q: %v", path, err)
	}

	tb.Cleanup(func() {
		err := db.Close()
		if err != nil {
			tb.Error("close archive DB:", err)
		}
	})

	return db
}

func openFS(tb testing.TB, path string, opts ...sqlarfs.Option) fs.FS {
	tb.Helper()
	db := openDB(tb, path)
	return sqlarfs.New(db, opts...)
}

func TestEmpty(t *testing.T) {
	ar := openFS(t, "testdata/empty.sqlar", sqlarfs.PermOwner)
	if err := fstest.TestFS(ar); err != nil {
		t.Fatal(err)
	}
	// Test a second time, without reopening.
	if err := fstest.TestFS(ar); err != nil {
		t.Fatal(err)
	}
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
		// t.Logf("%24q: %s", path, fs.FormatFileInfo(info))
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

	if err := fstest.TestFS(ar, "foo.txt", "bar.txt"); err != nil {
		t.Fatal(err)
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
		// t.Logf("%24q: %s", path, fs.FormatFileInfo(info))
		t.Logf("%24q: %s %11o %10d %-30s  %s", path, info.Mode(), uint32(info.Mode()), info.Size(), info.ModTime(), info.Name())
		return nil
	})

	if err := fstest.TestFS(ar, "a.txt", "b.txt", "subdir", "subdir/c.txt", "subdir/d.txt", "subdir/subdir2", "subdir/subdir2/e.txt", "subdir/subdir2/f.txt"); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDir(b *testing.B) {
	ar := openFS(b, "testdata/dir.sqlar", sqlarfs.PermOwner)
	files := []string{"a.txt", "b.txt", "subdir", "subdir/c.txt", "subdir/d.txt", "subdir/subdir2", "subdir/subdir2/e.txt", "subdir/subdir2/f.txt"}
	b.Run("first", func(b *testing.B) {
		if err := fstest.TestFS(ar, files...); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("others", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := fstest.TestFS(ar, files...); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// go test -run TestDirParallel -race -count=10
func TestDirParallel(t *testing.T) {
	ar := openFS(t, "testdata/dir.sqlar", sqlarfs.PermOwner)
	files := []string{"a.txt", "b.txt", "subdir", "subdir/c.txt", "subdir/d.txt", "subdir/subdir2", "subdir/subdir2/e.txt", "subdir/subdir2/f.txt"}

	nprocs := 2 * runtime.GOMAXPROCS(-1)

	for i := 0; i < nprocs; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			if err := fstest.TestFS(ar, files...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

package sqlarfs_test

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/dolmen-go/sqlar/sqlarfs"
)

var sqliteDriver string

func TestShowDriver(t *testing.T) {
	if sqliteDriver == "" {
		t.Fatal("Driver name: ∅")
	}
	t.Log("Driver name:", sqliteDriver)

	db := openDB(t, "testdata/empty.sqlar")

	driver := db.Driver()
	driverType := reflect.TypeOf(driver)
	if driverType.Kind() == reflect.Pointer {
		driverType = driverType.Elem()
	}
	t.Logf("Driver: %s (package: %s)", driverType, driverType.PkgPath())

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal("db.Conn:", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error("Conn.Close:", err)
		}
	})

	if err = conn.Raw(func(driverConn any) error {
		driverConnType := reflect.TypeOf(driverConn)
		if driverConnType.Kind() == reflect.Pointer {
			driverConnType = driverConnType.Elem()
		}
		t.Logf("Conn: %s (package: %s)", driverConnType, driverConnType.PkgPath())
		return nil
	}); err != nil {
		t.Fatal("conn.Raw:", err)
	}
}

func openDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()

	if sqliteDriver == "" {
		tb.Skip("Driver name: ∅")
	}

	// Open the DB in read-only mode for speed
	db, err := sql.Open(sqliteDriver, "file:"+path+"?mode=ro&immutable=1")
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

// BenchmarkDir aims to compare the impact of data caching on the second and others passes of [testing/fstest.TestFS]
// versus the first pass just after init.
func BenchmarkDir(b *testing.B) {
	db := openDB(b, "testdata/dir.sqlar")
	files := []string{"a.txt", "b.txt", "subdir", "subdir/c.txt", "subdir/d.txt", "subdir/subdir2", "subdir/subdir2/e.txt", "subdir/subdir2/f.txt"}
	var ar fs.FS

	// Benchmark the sqlarfs init and first pass of fstest.TestFS
	b.Run("firstTestFS", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ar = sqlarfs.New(db, sqlarfs.PermOwner)
			if err := fstest.TestFS(ar, files...); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Benchmark second and further passes of fstest.TestFS
	b.Run("othersTestFS", func(b *testing.B) {
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

func testPerms(t interface {
	Helper()
	Run(string, func(*testing.T)) bool
}, name string, perm sqlarfs.PermMask, files ...string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		ar := openFS(t, "testdata/perms.sqlar", perm)
		err := fstest.TestFS(ar, files...)

		// Because of Go issue #63707 we have to filter errors returned by TestFS.
		// https://github.com/golang/go/issues/63707

		var errs interface{ Unwrap() []error }
		switch {
		case err == nil: // ignore
		case errors.As(err, &errs):
			for _, err := range errs.Unwrap() {
				if errors.Is(err, fs.ErrPermission) {
					t.Logf("%T: %[1]q", err)
				} else {
					t.Fatal(err)
				}
			}
		case errors.Is(err, fs.ErrPermission):
			t.Logf("%T: %[1]q", err)
		default:
			// Because of Go issue #63675 we have to dig in the error's message.
			// https://github.com/golang/go/issues/63675
			t.Logf("%T: %[1]q", err)
			t.Log(err.Error())
			t.Log(fs.ErrPermission.Error())
			if strings.Contains(err.Error(), fs.ErrPermission.Error()) {
				t.Logf("%T: %[1]q", err)
				t.Log("Fallback to error message check")
				t.Skip("Skip. See https://github.com/golang/go/issues/63675")
			}
			t.Fatalf("%T: %[1]q", err)
		}
	})
}

func TestPerms(t *testing.T) {
	testPerms(t, "PermOwner", sqlarfs.PermOwner, "user", "user/u.txt")
	testPerms(t, "PermGroup", sqlarfs.PermGroup, "group", "group/g.txt")
	testPerms(t, "PermOthers", sqlarfs.PermOthers, "others", "others/o.txt")
}

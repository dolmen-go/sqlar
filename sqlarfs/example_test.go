package sqlarfs_test

import (
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"

	"github.com/dolmen-go/sqlar/sqlarfs"
)

func Example() {
	// Open DB in readonly mode for maximum speed
	db, err := sql.Open(sqliteDriver, "file:testdata/simple.sqlar?mode=ro&immutable=1")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// List files in the archive, recusively
	ar := sqlarfs.New(db, sqlarfs.PermOwner)
	fs.WalkDir(ar, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, _ := d.Info()
		fmt.Printf("%s %4d %s  %s\n", info.Mode(), info.Size(), info.ModTime().UTC().Format("_2 Jan 2006 15:04"), info.Name())
		return nil
	})

	// Dump one file
	f, err := ar.Open("foo.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fmt.Println("foo.txt:")
	io.Copy(os.Stdout, f)
	// Output:
	// dr-xr-xr-x    0  1 Jan 1970 00:00  .
	// -rw-r--r--    4 30 Sep 2023 14:51  bar.txt
	// -rw-r--r--    4 30 Sep 2023 16:14  foo.txt
	// foo.txt:
	// Foo
}

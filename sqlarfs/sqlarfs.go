// Package sqlarfs provides an implementation of [io/fs.FS] for [SQLite Archive Files].
//
// [SQLite Archive Files]: https://sqlite.org/sqlar.html
package sqlarfs

import (
	"bytes"
	"compress/flate"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// FS documents the [io/fs] interfaces implemented.
type FS interface {
	fs.FS
	fs.StatFS
	fs.ReadDirFS
}

// New returns an instance of [io/fs.FS] that allows to access the files in a SQLite Archive File opened with [database/sql].
func New(db *sql.DB) FS {
	return &arfs{db: db}
}

type arfs struct {
	db *sql.DB
}

var _ FS = (*arfs)(nil)

type fileinfo struct {
	name string
	// Note: mode is not portable "sqlite3 -Ac" uses the mode of the OS which varies
	// /Library/Developer/CommandLineTools/SDKs/MacOSX13.1.sdk/usr/include/sys/_types/_s_ifmt.h
	mode  uint32
	mtime int64
	sz    int64
}

// String implements interface [fmt.Stringer].
func (fi *fileinfo) String() string {
	return fmt.Sprintf("%s %10d %-30s  %s", fi.Mode(), fi.Size(), fi.ModTime(), fi.Name())
}

func (fi *fileinfo) scan(scan func(dest ...any) error) error {
	return scan(&fi.name, &fi.mode, &fi.mtime, &fi.sz)
}

// Name implements interface [fs.FileInfo].
func (fi *fileinfo) Name() string {
	return fi.name
}

// Size implements interface [fs.FileInfo].
func (fi *fileinfo) Size() int64 {
	return fi.sz
}

// Mode implements interface [fs.FileInfo].
func (fi *fileinfo) Mode() fs.FileMode {
	// Check conversion implementation for the os.Stat function:
	// sed -n '/^func fillFileStatFromSys/,/^}/p' $(go env GOROOT)/src/os/stat_*.go
	mode := fs.FileMode(fi.mode & 0777)
	switch fi.mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		mode |= fs.ModeDir
	case syscall.S_IFREG:
		// Do nothing
	}
	return mode
}

// ModTime implements interface [fs.FileInfo].
func (fi *fileinfo) ModTime() time.Time {
	return time.Unix(fi.mtime, 0)
}

// Sys implements interface [fs.FileInfo].
func (fi *fileinfo) Sys() any {
	return nil
}

// IsDir implements interface [fs.DirEntry].
func (fi *fileinfo) IsDir() bool {
	return fi.mode&syscall.S_IFDIR != 0
}

// Type implements interface [fs.DirEntry].
func (fi *fileinfo) Type() fs.FileMode {
	if fi.IsDir() {
		return fs.ModeDir
	}
	return 0
}

// Info implements interface [fs.DirEntry].
func (fi *fileinfo) Info() (fs.FileInfo, error) {
	return fi, nil
}

const escapeLikeChar = "§"

var escapeLike = strings.NewReplacer("%", escapeLikeChar+"%", "_", escapeLikeChar+"_", "!", escapeLikeChar+"!")

const (
	dirMode          uint32 = syscall.S_IFDIR | 0555
	sqlModeFilter           = `((mode&49152)>>9)<>0` // Skip files with broken mode: 49152 = syscall.S_IFREG|syscall.S_IFDIR
	sqlModeFilterDir        = `(mode&16384)<>0`      // 16384 = syscall.S_IFDIR => directories
	sqlModeFilterReg        = `(mode&32768)<>0`      // 32768 = syscall.S_IFREG => regular files
)

func (ar *arfs) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}
	if name == "." {
		name = ""
	} else {
		name = name + "/"
	}

	nameEsc := escapeLike.Replace(name)
	rows, err := ar.db.Query(``+
		// Files
		`SELECT SUBSTR(name,?),mode,mtime,sz`+
		` FROM sqlar`+
		` WHERE name LIKE ? ESCAPE '`+escapeLikeChar+`'`+
		` AND name NOT LIKE ? ESCAPE '`+escapeLikeChar+`'`+
		` AND `+sqlModeFilter+ // Skip files with broken mode
		` UNION ALL`+
		// Subdirectories: emulate entries from filenames in subdirs
		` SELECT DISTINCT SUBSTR(name, ?, INSTR(SUBSTR(name, ?), '/')-1),16749,0,0`+ // mode is: syscall.S_IFDIR | 0555
		` FROM sqlar`+
		` WHERE name LIKE ? ESCAPE '`+escapeLikeChar+`'`+
		` AND name NOT LIKE ? ESCAPE '`+escapeLikeChar+`'`,
		1+len(name),
		nameEsc+"_%",
		nameEsc+"%/%",
		1+len(name), 1+len(name),
		nameEsc+"_%/%",
		nameEsc+"%/%/%",
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var entries []fs.DirEntry
	var subdirs map[string]struct{}

	for rows.Next() {
		var fi fileinfo
		if err := fi.scan(rows.Scan); err != nil {
			return entries, err
		}
		// Some archives may have entries for directories
		// In that case we ignore the duplicates we created in the SQL
		if fi.IsDir() {
			if _, seen := subdirs[fi.name]; seen {
				continue
			}
			if subdirs == nil {
				subdirs = make(map[string]struct{})
			}
			subdirs[fi.name] = struct{}{}
		}
		entries = append(entries, &fi)
	}
	if err := rows.Err(); err != err {
		return entries, err
	}
	return entries, rows.Close()
}

type file struct {
	fs   *arfs
	info fileinfo
	r    io.ReadCloser
}

func (f *file) Stat() (fs.FileInfo, error) {
	return &f.info, nil
}

func (f *file) Read(b []byte) (int, error) {
	if f.r == nil {
		if f.fs == nil { // Closed
			return 0, io.EOF
		}
		if f.info.mode&0444 == 0 {
			return 0, fs.ErrPermission
		}
		var buf []byte
		if err := f.fs.db.QueryRow("SELECT data FROM sqlar WHERE name=? AND "+sqlModeFilterReg, f.info.name).Scan(&buf); err != nil {
			if err == sql.ErrNoRows {
				return 0, fs.ErrNotExist
			}
			return 0, err
		}
		if len(buf) == int(f.info.sz) {
			f.r = io.NopCloser(bytes.NewReader(buf))
		} else {
			f.r = flate.NewReader(bytes.NewReader(buf))
		}
	}
	return f.r.Read(b)
}

func (f *file) Close() error {
	r := f.r
	f.fs, f.r = nil, nil
	if r == nil {
		return nil
	}
	return r.Close()
}

// Stat implements interface [fs.StatFS].
func (ar *arfs) Stat(name string) (fs.FileInfo, error) {
	if name == "." {
		// FIXME Do a dummy query to ensure the sqlar table is available
		return &fileinfo{
			name:  ".",
			mode:  dirMode,
			mtime: 0,
			sz:    0,
		}, nil
	}

	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}

	var info fileinfo
	err := info.scan(
		ar.db.QueryRow(``+
			`SELECT name,mode,mtime,sz`+
			` FROM sqlar`+
			` WHERE name=?`+
			` AND `+sqlModeFilter+ // Skip file with broken mode
			` LIMIT 1`,
			name,
		).Scan)
	if err == sql.ErrNoRows {
		// Emulate directories like in ReadDir
		var ok bool
		if ar.db.QueryRow(``+
			`SELECT 1`+
			` FROM sqlar`+
			` WHERE SUBSTR(name,?)=?`+
			` LIMIT 1`,
			len(name)+1,
			name+"/",
		).Scan(&ok); err == nil && ok {
			info.mode = dirMode
		} else {
			return nil, fs.ErrNotExist
		}
	}
	_, info.name = filepath.Split(name)
	return &info, nil
}

// Open implements interface [fs.FS].
func (ar *arfs) Open(name string) (fs.File, error) {
	var ff file
	info, err := ar.Stat(name)
	if err != nil {
		return nil, err
	}
	ff.info = *(info.(*fileinfo))
	ff.fs = ar
	return &ff, nil
}

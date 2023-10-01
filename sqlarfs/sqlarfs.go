// Package sqlarfs provides an implementation of [io/fs.FS] for [SQLite Archive Files].
//
// [SQLite Archive Files]: https://sqlite.org/sqlar.html
package sqlarfs

import (
	"bytes"
	"compress/flate"
	"database/sql"
	"io"
	"io/fs"
	"strings"
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
	mode  fs.FileMode
	mtime int64
	sz    int64
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
	return fi.mode
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
	return fi.mode&fs.ModeDir != 0
}

// Type implements interface [fs.DirEntry].
func (fi *fileinfo) Type() fs.FileMode {
	return fi.mode
}

// Info implements interface [fs.DirEntry].
func (fi *fileinfo) Info() (fs.FileInfo, error) {
	return fi, nil
}

const escapeLikeChar = "§"

var escapeLike = strings.NewReplacer("%", escapeLikeChar+"%", "_", escapeLikeChar+"_", "!", escapeLikeChar+"!")

const dirMode = fs.ModeDir | 0555

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
		//` AND (mode>>16)=0`+ // Skip files with broken mode
		` UNION ALL`+
		// Subdirectories: entries created from filenames in subdirs
		` SELECT DISTINCT SUBSTR(name, ?, INSTR(SUBSTR(name, ?), '/')-1),2147484013,0,0`+ // mode is: fs.ModeDir | 0555
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
		if fi.mode.IsDir() {
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
		if err := f.fs.db.QueryRow("SELECT data FROM sqlar WHERE name=? AND (mode>>16)=0", f.info.name).Scan(&buf); err != nil {
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
		return io.EOF
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
			` AND (mode>>16)=0`+ // Skip file with broken mode
			` LIMIT 1`,
			name,
		).Scan)
	if err == sql.ErrNoRows {
		return nil, fs.ErrNotExist
	}
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

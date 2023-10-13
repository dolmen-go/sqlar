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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FS documents the [io/fs] interfaces provided by this implementation of [io/fs.FS].
type FS interface {
	fs.FS
	fs.StatFS
	fs.ReadDirFS
}

// New returns an instance of [io/fs.FS] that allows to access the files in an [SQLite Archive File] opened with [database/sql].
//
// db is an [sql/database] handle to the SQLite Archive file. Two drivers are known to work: [github.com/mattn/sqlite3] and [modernc.org/sqlite].
//
// The default permission mask used to enforce file permissions ('mode' column in the 'sqlar' table) is [PermAny].
//
// [SQLite Archive File]: https://sqlite.org/sqlar.html
func New(db *sql.DB, opts ...Option) FS {
	ar := &arfs{db: db, permMask: PermAny}
	for _, o := range opts {
		o.apply(ar)
	}
	return ar
}

type arfs struct {
	db       *sql.DB
	permMask PermMask

	dirInfo dirInfoCache
}

func (ar *arfs) canRead(mode uint32) bool {
	return mode&0444&uint32(ar.permMask) != 0
}

func (ar *arfs) canTraverse(mode uint32) bool {
	return mode&0111&uint32(ar.permMask) != 0
}

var _ FS = (*arfs)(nil)

// Option is an option for [New].
//
// Available options: [PermOwner], [PermGroup], [PermOthers], [PermAny].
type Option interface {
	apply(*arfs)
}

const (
	PermOwner  PermMask = 0700
	PermGroup  PermMask = 0070
	PermOthers PermMask = 0007
	PermAny    PermMask = 0777 // Allow to read (traverse for directories) any file that have at least one permission bit for either owner/group/others
)

// PermMask is a permission mask for enforcing [fs.FileMode] permissions
// (disallow to read files, disallow traversing or listing directories) in an SQLite Archive File.
//
// PermMask is an [Option] for [New].
type PermMask uint32

func (p PermMask) apply(ar *arfs) {
	switch p {
	// We do not accept any other values than the constants
	case PermOwner, PermGroup, PermOthers, PermAny:
		ar.permMask = p
	default:
		panic(fmt.Errorf("sqlar.New: invalid permission mask value"))
	}
}

// fileinfo implements interfaces [fs.FileInfo] and [fs.DirEntry].
type fileinfo struct {
	rowid uint64
	name  string
	// Note: mode is not io/fs.FileMode but instead the Unix S_IF* bits
	mode  uint32
	mtime int64
	sz    int64
}

var _ interface {
	fmt.Stringer
	fs.FileInfo
} = (*fileinfo)(nil)

// String implements interface [fmt.Stringer].
func (fi *fileinfo) String() string {
	return fs.FormatFileInfo(fi)
}

func (fi *fileinfo) scan(scan func(dest ...any) error) error {
	return scan(&fi.rowid, &fi.name, &fi.mode, &fi.mtime, &fi.sz)
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

// IsDir implements interface [fs.FileInfo].
func (fi *fileinfo) IsDir() bool {
	return fi.mode&syscall.S_IFDIR != 0
}

// ModTime implements interface [fs.FileInfo].
func (fi *fileinfo) ModTime() time.Time {
	return time.Unix(fi.mtime, 0)
}

// Sys implements interface [fs.FileInfo].
func (fi *fileinfo) Sys() any {
	return nil
}

type dirInfoCache struct {
	mu   sync.RWMutex
	info map[string]*fileinfo // Keys are directory paths validated with io/fs.ValidPath
}

func (di *dirInfoCache) load(path string) *fileinfo {
	di.mu.RLock()
	defer di.mu.RUnlock()
	return di.info[path]
}

func (di *dirInfoCache) store(path string, fi *fileinfo) *fileinfo {
	di.mu.Lock()
	defer di.mu.Unlock()
	if di.info == nil {
		di.info = make(map[string]*fileinfo)
	} else {
		// Keep the earlier value because a new one has been recently allocated
		if fi2 := di.info[path]; fi2 != nil {
			return fi2
		}
	}
	di.info[path] = fi
	return fi
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
	list, err := ar.readDir(name)
	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool {
			return list[i].Name() < list[j].Name()
		})
	}
	return list, err
}

func (ar *arfs) readDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}
	if name == "." {
		name = ""
	} else {
		fi, err := ar.stat(name)
		if err != nil {
			return nil, err
		}
		if !fi.IsDir() {
			return nil, fs.ErrInvalid
		}
		if !ar.canRead(fi.mode) {
			return nil, fs.ErrPermission
		}
		name = name + "/"
	}

	nameEsc := escapeLike.Replace(name)
	rows, err := ar.db.Query(``+
		// Files
		`SELECT rowid,SUBSTR(name,?),mode,mtime,sz`+
		` FROM sqlar`+
		` WHERE name LIKE ? ESCAPE '`+escapeLikeChar+`'`+
		` AND name NOT LIKE ? ESCAPE '`+escapeLikeChar+`'`+
		` AND `+sqlModeFilter+ // Skip files with broken mode
		` UNION ALL`+
		// Subdirectories: emulate entries from filenames in subdirs
		` SELECT DISTINCT 0,SUBSTR(name, ?, INSTR(SUBSTR(name, ?), '/')-1),16749,0,0`+ // mode is: syscall.S_IFDIR | 0555
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
		fi := new(fileinfo)
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
			fi = ar.dirInfo.store(name+"/"+fi.name, fi)
		}
		entries = append(entries, fs.FileInfoToDirEntry(fi))
	}

	if err := rows.Err(); err != err {
		return entries, err
	}
	return entries, rows.Close()
}

// Stat implements interface [fs.StatFS].
func (ar *arfs) Stat(name string) (fs.FileInfo, error) {
	if name == "." {
		info, err := ar.statRoot()
		if err != nil {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
		}
		return info, err
	}

	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}

	fi, err := ar.stat(name)
	if err != nil {
		// Avoid returning (*fileinfo)(nil) instead of (fs.FileInfo)(nil)
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	return fi, nil
}

var fileinfoRoot = fileinfo{
	rowid: 0,
	name:  ".",
	mode:  dirMode,
	mtime: 0,
	sz:    0,
}

func (ar *arfs) statRoot() (*fileinfo, error) {
	var fi *fileinfo
	fi = ar.dirInfo.load(".")
	if fi != nil {
		return fi, nil
	}
	fi = new(fileinfo)
	err := fi.scan(ar.db.QueryRow(`` +
		`SELECT rowid,'.',mode,mtime,sz` +
		` FROM sqlar` +
		` WHERE name='.'` +
		` LIMIT 1`).Scan)
	switch err {
	case sql.ErrNoRows:
		fi = &fileinfoRoot
		fallthrough
	case nil:
		return ar.dirInfo.store(".", fi), nil
	default:
		// Table sqlar doesn't exist or other error
		return nil, err
	}
}

// Stat implements interface [fs.StatFS].
func (ar *arfs) stat(name string) (*fileinfo, error) {
	dir, filename := filepath.Split(name)
	if dir == "" {
		fi, err := ar.statRoot()
		if err != nil {
			return nil, &fs.PathError{Op: "stat", Path: ".", Err: err}
		}
		if !ar.canTraverse(fi.mode) {
			return nil, fs.ErrPermission
		}
	} else {
		// Recursively check that we can traverse the tree
		// Note: dir has a trailing '/'
		fi, err := ar.stat(dir[:len(dir)-1])
		if err != nil {
			return nil, &fs.PathError{Op: "stat", Path: dir[:len(dir)-1], Err: err}
		}
		if !fi.IsDir() {
			return nil, fs.ErrNotExist
		}
		if !ar.canTraverse(fi.mode) {
			return nil, fs.ErrPermission
		}
	}

	info := ar.dirInfo.load(name)
	if info != nil {
		return info, nil
	}

	info = new(fileinfo)

	err := info.scan(
		ar.db.QueryRow(``+
			`SELECT rowid,name,mode,mtime,sz`+
			` FROM sqlar`+
			` WHERE name=?`+
			` AND `+sqlModeFilter+ // Skip file with broken mode
			` LIMIT 1`,
			name,
		).Scan)
	switch err {
	case nil:
		// OK
	case sql.ErrNoRows:
		// Emulate directories like in ReadDir
		var ok bool
		err = ar.db.QueryRow(``+
			`SELECT 1`+
			` FROM sqlar`+
			` WHERE SUBSTR(name,?)=?`+
			` LIMIT 1`,
			len(name)+1,
			name+"/",
		).Scan(&ok)
		switch {
		case err == nil && ok:
			info.mode = dirMode
		case err == sql.ErrNoRows || err == nil: // Case "err == nil" should never happen
			return nil, fs.ErrNotExist
		default:
			return nil, err
		}
	default:
		return nil, err
	}
	info.name = filename

	if info.IsDir() {
		info = ar.dirInfo.store(name, info)
	}

	return info, nil
}

// file gives access to a file in an SQLite Archive file.
//
// *file implements interface [fs.File].
type file struct {
	fs   *arfs
	path string
	info fileinfo
	r    io.ReadCloser
}

// dir gives access to a directory in an SQLite Archive file.
//
// *dir implements interface [fs.ReadDirFile].
type dir struct {
	file
	entries []fs.DirEntry
}

// Stat implements interface [fs.File].
func (f *file) Stat() (fs.FileInfo, error) {
	return &f.info, nil
}

// Read implements interface [fs.File].
func (f *file) Read(b []byte) (int, error) {
	if f.r == nil {
		if f.fs == nil { // Closed
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrClosed}
		}
		if !f.fs.canRead(f.info.mode) {
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrPermission}
		}
		var buf []byte
		err := f.fs.db.QueryRow(``+
			`SELECT data`+
			` FROM sqlar`+
			` WHERE rowid=?`+
			` AND `+sqlModeFilterReg,
			f.info.rowid,
		).Scan(&buf)
		switch err {
		case nil:
			// OK
		case sql.ErrNoRows:
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrNotExist}
		default:
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: err}
		}
		if len(buf) == int(f.info.sz) {
			f.r = io.NopCloser(bytes.NewReader(buf))
		} else {
			f.r = flate.NewReader(bytes.NewReader(buf))
		}
	}
	return f.r.Read(b)
}

// Close implements interface [fs.File].
func (f *file) Close() error {
	r := f.r
	f.fs, f.r = nil, nil
	if r == nil {
		return nil
	}
	return r.Close()
}

// ReadDir implements interface [fs.ReadDirFile].
func (d *dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.file.fs == nil {
		return nil, fs.ErrClosed
	}

	// FIXME naive implementation

	if d.entries == nil {
		var err error
		d.entries, err = d.file.fs.readDir(d.file.path)
		if err != nil {
			return nil, err
		}
	}
	if n <= 0 {
		e := d.entries
		d.entries = []fs.DirEntry{}
		return e, nil
	}
	if len(d.entries) <= n {
		e := d.entries
		d.entries = []fs.DirEntry{}
		return e, io.EOF
	}
	// TODO make a copy and clear the original (to free entries)
	e := d.entries[:n]
	d.entries = d.entries[n:]
	return e, nil
}

// Open implements interface [fs.FS].
func (ar *arfs) Open(name string) (fs.File, error) {
	var info *fileinfo
	var err error
	if name == "." {
		info, err = ar.statRoot()
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	} else {
		if !fs.ValidPath(name) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
		}
		info, err = ar.stat(name)
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	}

	if info.IsDir() {
		return &dir{file: file{fs: ar, info: *info, path: name}}, nil
	}

	return &file{fs: ar, info: *info, path: name}, nil
}

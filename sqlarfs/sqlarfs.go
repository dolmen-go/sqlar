// Package sqlarfs provides an implementation of [io/fs.FS] for [SQLite Archive Files] via [database/sql].
//
// [SQLite Archive Files]: https://sqlite.org/sqlar.html
package sqlarfs

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"io/fs"
	"path"
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
	fs.ReadLinkFS
}

// Rows is the interface of [*database/sql.Rows].
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// QueryRowsFunc is the type of the function used to query rows in the database, like [database/sql.DB.Query].
type QueryRowsFunc[R Rows] = func(query string, args ...any) (R, error)

// New returns an instance of [io/fs.FS] that allows to access the files in an [SQLite Archive File] opened with [database/sql].
//
// queryRows allows to run an SQL query in the SQLite Archive file. See [database/sql.DB.Query] for an example of such a function.
//
// sqlarfs uses caching for the directory structure, and so it assumes
// that the sqlar table is not modified while browsing the filesystem. So if the sqlar table is modified, create a new instance.
//
// For maximum performance, open the SQLite database in read-only, immutable mode:
//
//	file:<path>?mode=ro&immutable=1
//
// The default permission mask used to enforce file permissions ('mode' column in the 'sqlar' table) is [PermAny].
// Others masks are available: [PermOwner], [PermGroup], [PermOthers] as [Option].
//
// [SQLite Archive File]: https://sqlite.org/sqlar.html
func New[R Rows](queryRows QueryRowsFunc[R], opts ...Option) FS {
	ar := &arfs[R]{queryRows: queryRows, permMask: PermAny}
	for _, o := range opts {
		o.apply(&ar.permMask)
	}
	return ar
}

type arfs[R Rows] struct {
	queryRows QueryRowsFunc[R]
	permMask  PermMask

	dirInfo dirInfoCache
}

func (ar *arfs[_]) canRead(mode uint32) bool {
	return mode&0444&uint32(ar.permMask) != 0
}

func (ar *arfs[_]) canTraverse(mode uint32) bool {
	return mode&0111&uint32(ar.permMask) != 0
}

// queryRow implements [database/sql.DB.QueryRow] semantics using a [QueryRowsFunc].
func (ar *arfs[R]) queryRow(query string, args []any, results []any) (bool, error) {
	rows, err := ar.queryRows(query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, rows.Err()
	}
	if err := rows.Scan(results...); err != nil {
		return false, err
	}
	return true, nil
}

var _ FS = (*arfs[Rows])(nil)

// Option is an option for [New].
//
// Available options: [PermOwner], [PermGroup], [PermOthers], [PermAny].
type Option interface {
	apply(*PermMask)
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

func (p PermMask) apply(permMask *PermMask) {
	switch p {
	// We do not accept any other values than the constants
	case PermOwner, PermGroup, PermOthers, PermAny:
		*permMask = p
	default:
		panic(fmt.Errorf("sqlar.New: invalid permission mask value"))
	}
}

// fileinfo implements interfaces [fs.FileInfo] and [fs.DirEntry].
type fileinfo struct {
	name string
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

func (fi *fileinfo) scanners() []any {
	return []any{&fi.name, &fi.mode, &fi.mtime, &fi.sz}
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
	case syscall.S_IFLNK:
		mode |= fs.ModeSymlink
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

func (ar *arfs[_]) ReadDir(name string) ([]fs.DirEntry, error) {
	list, err := ar.readDir(name)
	if len(list) > 0 {
		sort.Slice(list, func(i, j int) bool {
			return list[i].Name() < list[j].Name()
		})
	}
	if err != nil {
		return list, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	return list, nil
}

func (ar *arfs[_]) readDir(name string) ([]fs.DirEntry, error) {
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
	rows, err := ar.queryRows(``+
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
		fi := new(fileinfo)
		if err := rows.Scan(fi.scanners()...); err != nil {
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

	if err := rows.Err(); err != nil {
		return entries, err
	}
	return entries, rows.Close()
}

// Stat implements interface [fs.StatFS].
func (ar *arfs[_]) Stat(name string) (fs.FileInfo, error) {
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

	_, fi, err := ar.resolve(name)
	if err != nil {
		// Avoid returning (*fileinfo)(nil) instead of (fs.FileInfo)(nil)
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	return fi, nil
}

// Lstat implements interface [fs.ReadLinkFS].
func (ar *arfs[_]) Lstat(name string) (fs.FileInfo, error) {
	if name == "." {
		return ar.Stat(name)
	}

	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrInvalid}
	}

	fi, err := ar.stat(name)
	if err != nil {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: err}
	}
	return fi, nil
}

var fileinfoRoot = fileinfo{
	name:  ".",
	mode:  dirMode,
	mtime: 0,
	sz:    0,
}

func (ar *arfs[_]) statRoot() (*fileinfo, error) {
	var fi *fileinfo
	fi = ar.dirInfo.load(".")
	if fi != nil {
		return fi, nil
	}

	fi = new(fileinfo)
	hasRow, err := ar.queryRow(``+
		`SELECT '.',mode,mtime,sz`+
		` FROM sqlar`+
		` WHERE name='.'`+
		` LIMIT 1`,
		nil, fi.scanners())

	if err != nil {
		// Table sqlar doesn't exist or other error
		return nil, err
	}

	if !hasRow {
		fi = &fileinfoRoot
	}
	return ar.dirInfo.store(".", fi), nil
}

// stat returns the [fileinfo] of the entry named name.
// It doesn't follow symlinks.
func (ar *arfs[_]) stat(name string) (*fileinfo, error) {
	dir, filename := path.Split(name)
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

	hasRow, err := ar.queryRow(``+
		`SELECT name,mode,mtime,sz`+
		` FROM sqlar`+
		` WHERE name=?`+
		` AND `+sqlModeFilter+ // Skip file with broken mode
		` LIMIT 1`,
		[]any{name},
		info.scanners(),
	)
	if err != nil {
		return nil, err
	}
	if !hasRow {
		// Emulate directories like in ReadDir
		var ok bool
		hasRow, err = ar.queryRow(``+
			`SELECT 1`+
			` FROM sqlar`+
			` WHERE SUBSTR(name,?)=?`+
			` LIMIT 1`,
			[]any{len(name) + 1, name + "/"},
			[]any{&ok},
		)
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return nil, fs.ErrNotExist
		}
		info.mode = dirMode
	}
	info.name = filename
	if info.IsDir() {
		info = ar.dirInfo.store(name, info)
	}
	return info, nil
}

func (ar *arfs[_]) resolve(name string) (string, *fileinfo, error) {
	const maxSymlinks = 10
	currPath := name
	for i := 0; i < maxSymlinks; i++ {
		fi, err := ar.stat(currPath)
		if err != nil {
			return "", nil, err
		}
		if fi.mode&syscall.S_IFMT != syscall.S_IFLNK {
			return currPath, fi, nil
		}

		target, err := ar.ReadLink(currPath)
		if err != nil {
			return "", nil, err
		}
		target = path.Clean(path.Join(path.Dir(currPath), target))
		if !fs.ValidPath(target) {
			return "", nil, &fs.PathError{Op: "readlink", Path: currPath, Err: fs.ErrInvalid}
		}
		currPath = target
	}
	return "", nil, fmt.Errorf("too many symlinks")
}

// file gives access to a file in an SQLite Archive file.
//
// *file implements interface [fs.File].
type file[R Rows] struct {
	fs   *arfs[R]
	info fileinfo
	path string
	r    io.ReadCloser
}

// dir gives access to a directory in an SQLite Archive file.
//
// *dir implements interface [fs.ReadDirFile].
type dir[R Rows] struct {
	file[R]
	entries []fs.DirEntry
}

// Stat implements interface [fs.File].
func (f *file[_]) Stat() (fs.FileInfo, error) {
	return &f.info, nil
}

// Read implements interface [fs.File].
func (f *file[_]) Read(b []byte) (int, error) {
	if f.r == nil {
		if f.fs == nil { // Closed
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrClosed}
		}
		if !f.fs.canRead(f.info.mode) {
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrPermission}
		}
		var buf []byte
		hasRow, err := f.fs.queryRow(``+
			`SELECT data`+
			` FROM sqlar`+
			` WHERE name=?`+
			` AND `+sqlModeFilterReg,
			[]any{f.path},
			[]any{&buf},
		)
		if err != nil {
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: err}
		}
		if !hasRow {
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrNotExist}
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
func (f *file[_]) Close() error {
	r := f.r
	f.fs, f.r = nil, nil
	if r == nil {
		return nil
	}
	return r.Close()
}

// ReadDir implements interface [fs.ReadDirFile].
func (d *dir[_]) ReadDir(n int) ([]fs.DirEntry, error) {
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

// ReadLink implements interface [fs.ReadLinkFS].
func (ar *arfs[_]) ReadLink(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	info, err := ar.stat(name)
	if err != nil {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: err}
	}
	if info.mode&syscall.S_IFMT != syscall.S_IFLNK {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	if !ar.canRead(info.mode) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrPermission}
	}

	var target string
	hasRow, err := ar.queryRow(``+
		`SELECT data`+
		` FROM sqlar`+
		` WHERE name=?`,
		[]any{name},
		[]any{&target},
	)
	if err != nil {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: err}
	}
	if !hasRow {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrNotExist}
	}

	if strings.HasPrefix(target, "/") {
		// Absolute paths in archives are not allowed
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	p := path.Clean(path.Join(path.Dir(name), target))
	if !fs.ValidPath(p) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}

	return target, nil
}

// Open implements interface [fs.FS].
func (ar *arfs[R]) Open(name string) (fs.File, error) {
	var info *fileinfo
	var path string
	var err error
	if name == "." {
		info, err = ar.statRoot()
		path = "."
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	} else {
		if !fs.ValidPath(name) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
		}
		path, info, err = ar.resolve(name)
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	}

	if info.IsDir() {
		return &dir[R]{file: file[R]{fs: ar, info: *info, path: path}}, nil
	}

	return &file[R]{fs: ar, info: *info, path: path}, nil
}

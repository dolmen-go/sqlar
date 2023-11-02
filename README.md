
# sqlar - Go libraries for [SQLite Archive Files](https://sqlite.org/sqlar.html)

## About SQLite Archive Files

See https://sqlite.org/sqlar.html

## Status

[`v0.2.0`](https://pkg.go.dev/github.com/dolmen-go/sqlar@v0.2.0) should be production ready.

The implementation of [`sqlarfs`](https://pkg.go/dev/github.com/dolmen-go/sqlar/sqlarfs) is quite
naive so far:
  * the DB is queried on every directory read, almost without caching (`v0.2.0`` has only a cache for [FileInfo](https://pkg.go.dev/io/fs.FileInfo) of directory entries).
  * file data is entirely read in memory on first read of a file.
  * reading file data is done purely through the SQL layer via the
    [database/sql](https://pkg.go.dev/database/sql) package. The C implementation of
    SQLite has a [BLOB API](https://sqlite.org/c3ref/blob_open.html) but we aren't using it.

So it is not yet recommended for thousands of files (but not yet really tested).
Reports of performance issues and use cases are very welcome.

## Doc

Package [github.com/dolmen-go/sqlar/sqlarfs](https://pkg.go.dev/github.com/dolmen-go/sqlar/sqlarfs) implements interface
[io/fs.FS](https://pkg.go.dev/io/fs#FS).

## License

Copyright 2023 Olivier Mengué

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

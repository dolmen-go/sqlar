
empty.sqlar:
	[ -d empty ] || mkdir -m 555 empty
	cd empty ; sqlite3 ../$@ -Ac
	sqlite3 -box $@ 'SELECT name, mode, mtime, sz FROM sqlar ORDER BY name'

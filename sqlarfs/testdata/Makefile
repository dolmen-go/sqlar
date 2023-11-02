
empty.sqlar:
	[ -d empty ] || mkdir -m 555 empty
	cd empty ; sqlite3 ../$@ -Ac
	sqlite3 -box $@ 'SELECT name, mode, mtime, sz FROM sqlar ORDER BY name'


perms.sqlar: date = 2023-10-20T00:06:03
perms.sqlar: dirs = user group others
perms.sqlar: files = user/u.txt group/g.txt others/o.txt
perms.sqlar:
	[ -d perms ] || mkdir -m 755 perms
	cd perms ; mkdir $(dirs) ; touch -d $(date) $(files) $(dirs) && chmod 444 $(files)
	cd perms ; sqlite3 ../$@ -Ac $(dirs)
	sqlite3 -box $@ 'SELECT name, lsmode(mode), mtime, sz FROM sqlar ORDER BY name'
	sqlite3 $@ 'UPDATE sqlar SET mode = 0x4000 | (7<<6) WHERE name = '"'user'"
	sqlite3 $@ 'UPDATE sqlar SET mode = 0x4000 | (7<<3) WHERE name = '"'group'"
	sqlite3 $@ 'UPDATE sqlar SET mode = 0x4000 | 7 WHERE name = '"'others'"
	sqlite3 -box $@ 'SELECT name, lsmode(mode), mtime, sz FROM sqlar ORDER BY name'


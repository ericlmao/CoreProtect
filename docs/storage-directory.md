# Storage Directory

CoreProtect keeps its configuration and its data in two different places. The plugin folder,
`plugins/CoreProtect/`, holds only files an operator edits. Everything CoreProtect persists lives in
`storage/CoreProtect/`, alongside `plugins/` in your server directory.

Keeping the two apart means a plugin folder can be copied between servers, zipped up for a support
request, or committed to configuration management without carrying a multi-gigabyte history or a
database password with it.

## What lives where

`plugins/CoreProtect/`

| File | Purpose |
| --- | --- |
| `config.yml` | Logging and lookup settings |
| `<world>.yml` | Per-world overrides |
| `language.yml` | Translated phrases you can edit |
| `blacklist.txt` | Users, blocks and commands to skip |

`storage/CoreProtect/`

| File | Purpose |
| --- | --- |
| `database.yml` | Which database engine to use, and how to reach it |
| `database.db` | The SQLite database, with its `-wal`, `-shm` and `-journal` sidecars |
| `database.duckdb` | The DuckDB database, with its `.wal` sidecar and `.tmp` spill directory |
| `database.db.v1-<timestamp>` | A database set aside by an upgrade |
| `old.db` | An upstream database placed here to be imported |
| `.clickhouse-writer` | Records which server is writing to a shared ClickHouse database |
| `.language` | Cached translations, written by CoreProtect |
| `cache/` | Scratch space, used only when the system temporary directory rejects executables |

These paths are fixed. No configuration key names a database file, because a mistyped path is a
silently empty history rather than an error you would notice.

## Database settings

The engine selection and the credentials it needs are in `storage/CoreProtect/database.yml`:

```yaml
database-type: duckdb
table-prefix: co_
mysql-host: 127.0.0.1
mysql-port: 3306
mysql-database: database
mysql-username: root
mysql-password: ""
clickhouse-host: 127.0.0.1
clickhouse-port: 8123
clickhouse-database: default
clickhouse-username: default
clickhouse-password: ""
clickhouse-tls: false
```

Everything else stays in `config.yml`, including the DuckDB resource limits and the compression
settings, which describe how CoreProtect behaves rather than which server it connects to.

## Upgrading from an older version

Older versions of CoreProtect kept all of this inside the plugin folder. The move happens by itself,
on the first start after upgrading, before anything else runs. Each file is moved rather than
copied, so it needs no free disk space, and a database always travels together with its write-ahead
log so no committed transaction is left behind.

Nothing is ever deleted. If a file already exists in `storage/CoreProtect/`, the storage copy is the
one CoreProtect uses, and the copy in the plugin folder is renamed to
`<name>.legacy-<timestamp>` and left there for you. A line in the server log names both files. Once
you are satisfied the renamed file holds nothing you need, you can delete it.

The database settings are moved out of `config.yml` in the same way, and your own comments move with
the settings they describe. If `database.yml` already existed, the settings still in `config.yml`
are written to `config.yml.database-legacy` and an error is logged, because a server that connects
to the wrong database records nothing rather than failing loudly. Check that `database.yml` names
the database you meant before deleting that file.

If an earlier attempt was interrupted while a database and its sidecars were in flight, CoreProtect
finds a `database.db.migration-incomplete` marker on the next start and refuses to open the database
rather than opening one that may be missing committed transactions. Put the database file back
together with its `-wal`, `-shm` and `-journal` sidecars in one directory, delete the marker, and
start the server again.

Migration runs on every start and does nothing once there is nothing left to move, so restarting
part way through an upgrade is safe.

### Left behind by an upgrade

One directory is not moved for you. Older versions pointed Java's temporary directory at `cache/` in
your server directory when the system temporary directory would not hold executable files.
CoreProtect now uses `storage/CoreProtect/cache/` instead, but the old directory is left alone
because `cache` is a common enough name that CoreProtect cannot prove it created it. It holds
nothing but scratch files and can be deleted once CoreProtect has started successfully.

# Automatic Purging

Automatic purging removes old CoreProtect data on a daily schedule, helping keep database growth under control without requiring manual `/co purge` runs.

Automatic purging is always enabled. The `auto-purge` option controls how much history is kept, rather than whether the feature runs at all.

## Configuration

Set `auto-purge` in `config.yml` to the amount of data you want to keep:

```yaml
auto-purge: 180d
```

This example keeps the most recent 180 days of CoreProtect data and automatically removes older data.

Supported values use the same style as CoreProtect command times, such as `30d`, `12w`, or `6mo`. A plain number, such as `180`, is read as a number of days. The minimum automatic purge value is `30d`; smaller values are raised to it. If the value is missing or cannot be read, the default retention of 180 days is used.

## Schedule

Automatic purging runs once per day using your server's local time. By default, it runs at midnight.

To change the daily runtime, set `auto-purge-time` in `config.yml`:

```yaml
auto-purge-time: 03:30
```

Use 24-hour `HH:mm` server time.

After changing `auto-purge` or `auto-purge-time`, use `/co reload` or restart the server. Changes apply to the next scheduled run.

CoreProtect logs the next scheduled run when the server starts and again after an automatic purge completes.

## How It Works

Automatic purging runs in the background. SQLite, MySQL, and DuckDB remove old rows incrementally in small batches with short pauses between database work, exactly as a manual purge does. ClickHouse uses its columnar retention path, dropping fully covered monthly partitions for an unfiltered time purge and synchronously removing rows from any partially covered partition.

On SQLite, the automatic purge also packs activity that has aged out of the hot window into compressed storage, and removes expired compressed data by dropping whole segments. See the [storage layout guide](/storage-layout/).

Rows are always deleted in place. Neither an automatic nor a manual purge ever copies the database into a second file, so purging does not require additional free disk space and cannot leave a duplicate database behind.

Automatic purging does not optimize MySQL or run a ClickHouse `OPTIMIZE FINAL`. SQLite truncates its write-ahead log and DuckDB performs a checkpoint after cleanup. Deleting rows frees space for reuse inside the database, but it may not immediately reduce the database file or table size on disk.

Only one automatic purge can run per CoreProtect instance at a time. If the server shuts down, a manual purge starts, a database migration or conversion starts, or the consumer is manually paused, the automatic purge stops safely at the next batch. Because each run deletes everything older than the retention period, a partially completed purge simply finishes its work during the next scheduled run.

A manual `/co purge` takes priority: it asks a running automatic purge to stop, then proceeds once the database is free.

ClickHouse rejects automatic and manual purges while `database-lock` is disabled. For a shared namespace, stop every installation, enable `database-lock` on one installation, restart or reload it, and finish the purge before restoring shared-writer mode.

## Existing Databases

If enabling automatic purging on a new database, no additional action is required.

On an existing database, the first automatic run removes everything older than the retention period, which can take a while. You can instead run a manual purge first using the same time value:

```text
/co purge t:180d
```

For MySQL, add `#optimize` if you want to reclaim disk space during the initial manual purge:

```text
/co purge t:180d #optimize
```

ClickHouse normally reclaims complete old partitions without `#optimize`. A manual ClickHouse purge accepts `#optimize`, but it runs the expensive `OPTIMIZE TABLE ... FINAL` operation and is generally unnecessary. DuckDB checkpoints automatically after a manual or automatic purge; `#optimize` has no additional effect for DuckDB or SQLite.

## Status

Use `/co status` to see how many rows have been automatically purged since the last server restart.

## Troubleshooting

**Automatic purging doesn't appear to run:**

* Verify `auto-purge` is set to a valid value of at least `30d`
* Verify `auto-purge-time` uses 24-hour `HH:mm` format
* Check the server console for automatic purge scheduling messages

# Storage Layout

SQLite installations use a two tier storage layout designed to hold as much history as possible in as little disk space as possible.

## How it works

**Recent activity** stays in the ordinary tables, fully indexed. Lookups, rollbacks and the inspector work exactly as they always have, at the same speed.

**Older activity** is packed into compressed segments once per day. A segment holds up to 65,536 consecutive rows from a single day, stored column by column instead of row by row: timestamps and row ids become small differences from the previous row, the interned world, user, type and action numbers pack into one or two bytes each, and the item and block payloads are compressed together with a dictionary trained on that server's own data. Segments carry no indexes, and a segment records the range of times, worlds and chunks it covers so a lookup can skip the ones that cannot match.

Measured on a database of 540,000 events with realistic item metadata, the whole file went from **77.9 MB to 6.9 MB — 11.3x smaller**. Most of that comes from what disappears: per-row storage overhead and the indexes, which on a history heavy database outweigh the event data itself.

The trade is search speed on old data. A lookup that reaches into compressed storage decodes the segments it cannot rule out, which takes seconds rather than milliseconds. Lookups that only cover the hot window never touch compressed storage at all.

## Configuration

```yaml
hot-window: 7d
blob-compression: true
blob-compression-level: 19
hot-blob-compression-level: 3
```

`hot-window` decides how much history stays in the fast tables. A longer window keeps more data instantly searchable and uses more disk; a shorter one compresses sooner. Values use the usual CoreProtect time style, such as `3d`, `7d` or `30d`.

`blob-compression-level` is used when data is packed into segments, and `hot-blob-compression-level` when it is first logged. The low level for fresh data keeps logging cheap, since that data is recompressed properly within days anyway.

## What still works unchanged

* Lookups, rollbacks, restores and the inspector read compressed and live data together, in one result.
* Row ids are preserved, so paging through a long lookup behaves as before.
* Rolling back data that has already been compressed is supported. The rollback state is recorded alongside the segment; segments themselves are never rewritten.
* `/co purge` and automatic purging remove compressed data by dropping whole segments, which is far cheaper than deleting rows. Because segments never span a day, retention lands exactly on day boundaries.

## Compacting on demand

Compression normally happens during the nightly automatic purge. To pack older data immediately — after lowering `hot-window`, or just to see the effect — run:

```text
/co compact
```

Unlike the nightly run, `/co compact` ignores `hot-window` entirely and packs everything logged up to that moment, including data from today. It reports how many rows were packed and returns the freed pages to the file system afterwards. Newly logged data goes back into the live tables as usual, so the hot window still governs everything written after the compact.

## Limits

* This layout is SQLite only. DuckDB and ClickHouse compress their own storage; MySQL is unchanged.
* A purge limited to one world or to specific block types only covers data still in the live tables, since removing part of a segment would mean rewriting it.
* Live entity data (`entity_spawn`) is never compressed. It is a registry of current entities rather than a log, and it is updated at any age.

## Upgrading an existing database

The layout is not compatible with databases written by upstream CoreProtect, and there is no conversion. On first start, an older database file is renamed to `database.db.v1-<timestamp>` and a new, empty database takes its place. Renaming moves the file rather than copying it, so this needs no extra disk space and nothing is deleted — the old file stays where it is until you remove it yourself.

If you want your old history, keep running upstream CoreProtect against the renamed file, or delete it once you no longer need it.

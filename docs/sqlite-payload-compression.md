# SQLite Payload Compression

This fork can store selected large SQLite payload BLOBs in `co_payload` and keep the normal lookup columns inline. The feature is disabled by default.

Compressed or deduplicated payloads:

* `co_block.meta`
* `co_block.blockdata`
* `co_container.metadata`
* `co_item.data`
* `co_entity.data`

Intentionally not compressed:

* time, user, world, x/y/z, action, type, amount, and rollback state columns
* columns used by SQL filters, ordering, joins, or indexes
* sign text, because sign lookups currently filter with `LENGTH(line_*)` in SQL

Configuration:

```yaml
database.sqlite.payload-compression.enabled: false
database.sqlite.payload-compression.codec: zstd
database.sqlite.payload-compression.zstd-level: 3
database.sqlite.payload-compression.min-bytes: 128
database.sqlite.payload-compression.deduplicate: true
database.sqlite.payload-compression.store-raw-if-larger: true
database.sqlite.payload-compression.keep-legacy-inline-values: true
```

When enabled, new SQLite writes store the selected payloads through `co_payload` and write nullable payload reference columns next to the legacy inline columns. Reads prefer the payload id when present and fall back to the legacy inline value otherwise, so old rows remain readable. Compression uses `zstd-jni` because it provides a small Java API around Zstandard with good size/speed tradeoffs for serialized item and entity payloads.

Keep `keep-legacy-inline-values` enabled for the safest rollout. Disabling it allows new/backfilled rows to clear legacy inline BLOBs after writing a payload id, which saves more space but relies on every read path using the payload resolver.

Backfill:

```text
/co payload-backfill dry-run
/co payload-backfill batch:1000
```

Backfill runs on a background thread, scans old inline values in batches, writes them into `co_payload`, and updates the matching payload id column. It is safe to re-run. It does not run automatically at startup. Take a database backup before enabling compression or running backfill.

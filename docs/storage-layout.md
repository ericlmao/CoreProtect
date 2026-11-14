# Storage Layout

SQLite installations store activity in a layout designed to hold as much history as possible in as little disk space as possible, while every lookup stays fast.

## How it works

Activity is written to the ordinary tables first, and packed into compressed segments as soon as there is enough of it: a segment is written the moment a day has logged 65,536 rows of one kind, and whatever a day logged beyond its last full segment is packed a few minutes after the day ends. The ordinary tables are only where rows wait until then. There is no window of recent history kept apart and nothing to configure: every lookup, rollback, restore and inspector click reads the waiting rows and the packed ones together, in one result, and nothing about the answer depends on where a row happens to be.

A segment stores its rows column by column instead of row by row. Timestamps and row ids become small differences from the previous row, the interned world, user, type and action numbers pack into one or two bytes each, and the item and block payloads are compressed together with a dictionary trained on that server's own data. Segments carry no indexes. Instead:

* each segment records the range of times, worlds, players and block types it covers, and exactly how many of its rows lie in each chunk, so a lookup skips every segment that cannot match, and a lookup covering whole chunks is counted without reading any rows;
* inside a segment, rows are ordered by where they happened and divided into groups of 1,024 rows, each recording the range of every column it holds and the chunks its rows lie in, so a lookup about one place decodes only the groups holding rows there rather than the whole segment. Groups are compressed eight at a time into units, and each unit is stored as a row of its own beside the segment, so such a lookup also reads only the units holding those groups from the database, a small part of the segment, rather than all of it;
* recently read segments are kept in memory (`lookup-cache-mb`), so repeated clicks, paging and a rollback after its preview do not read them again.

Chat, commands and sessions are looked up by player rather than by place, and are ordered by player instead.

Measured on segments taken from a production server, the current format stores its busiest table, container transactions, in half the space the previous format did, and a lookup about one place reads a segment three to nine times faster. Keeping the waiting area small also keeps writes cheap: the indexes on the ordinary tables only ever cover the rows that have not been packed yet, where on the same server a single day of unpacked container rows had grown indexes larger than the rows themselves.

## Configuration

```yaml
blob-compression: true
blob-compression-level: 19
log-blob-compression-level: 3
lookup-cache-mb: 128
```

`blob-compression-level` is used when activity is packed into segments, and `log-blob-compression-level` for item and block data as it is logged. The low level for fresh data keeps logging cheap, since it is packed properly soon after anyway. `lookup-cache-mb` is the memory kept for recently read segments.

Earlier builds had a `hot-window` setting that kept a number of days unpacked. It no longer exists and is ignored if present.

## What still works unchanged

* Lookups, rollbacks, restores, the inspector and the API read packed and waiting activity together, in one result.
* Row ids are preserved, so paging through a long lookup behaves as before.
* Rolling back data that has already been packed is supported. The rollback state is recorded alongside the segment; the segment's rows are never changed.
* `/co purge` and automatic purging remove packed data by dropping whole segments, which is far cheaper than deleting rows. Because segments never span a day, retention lands exactly on day boundaries.

## Packing on demand

Packing runs on its own in the background, a segment at a time, and gives way to purges, rollbacks and shutdown. To pack everything logged so far immediately, including the part that has not filled a segment yet, run:

```text
/co compact
```

It reports how many rows were packed and returns the freed pages to the file system afterwards.

## Compacting days that are over

A busy table writes a segment every few minutes, hundreds a day, each ordered by place only within itself, so the activity at one chest over one day is spread across every segment written while anyone used it. Once a day is over nothing more is written to it, and its segments are merged into a few of about 262,144 rows each, ordered by place across the whole day. The rows of one place over one day then lie together in one segment, and a lookup about a place opens about one segment for each day the place saw activity instead of one for every few minutes. On a month of busy container activity that took an inspector click from about 8,900 segments and 150 MB read to about 30 segments and 3.4 MB.

Compaction runs in the background whenever there is nothing to pack or rewrite, one day at a time, and all at once in `/co compact` and the nightly maintenance run. Only segments that follow one another, with nothing else of the table in between, are merged, so a row packed late on its own is never merged across. The merged segments are written aside and then take the place of the ones they were made from in one short transaction, while no lookup is running, so a lookup sees one or the other and never both or neither; a compaction that is interrupted leaves the original segments as they were. Row ids, values and rollback state do not change. A compacted segment keeps each unit's share of the item and block data beside the unit, so a lookup reads the share holding the rows it keeps rather than the data of the whole segment.

Because a compacted segment spans its whole day, a purge drops a compacted day once the cutoff has passed the end of it.

## Upgrading segments written by earlier builds

Segments written before rows were grouped, before groups listed their chunks, or before units were stored apart from the rest of the segment, are still read. They are rewritten in the current format in the background, a few at a time whenever there is nothing else to pack, and all at once by `/co compact` and the nightly maintenance run. A segment that only needs its units moved out is split as it is, without being decoded or compressed again. Each segment is rewritten in its own short transaction and checked before it is written; its rows, their ids and their rollback state do not change, and lookups read it the same way before, during and after.

## Entity data

Most tables are packed into segments as they are logged. Entity data is the exception. A lookup fetches it a row at a time by row id, from anywhere in the table, so it has to
stay somewhere a single row can be read on its own — unpacking a segment of tens of thousands of
rows to display one entity would be far worse than the space it saves.

It is compressed in place instead. Each blob is only a couple of kilobytes, and what repeats in this
data repeats between rows rather than within any one of them: the same entity types, the same
attribute names, the same class descriptions, over and over. Compressed on its own such a blob
shrinks by about a third. Compressed against a dictionary trained on a sample of the same server's
data, the same blobs shrink to about a thirtieth, and each one stays independently readable.

Dictionaries are stored in the database next to the segment dictionaries and are never removed, so
blobs written before one existed, or against an earlier one, keep reading back. Each blob records
which dictionary produced it.

Better still, the blobs of neighbouring rows are compressed **together**, sixty four to a frame. A
blob on its own is worth about thirty times; sixty four of them together are worth about seventy
eight, because the compressor can see across rows as well as within them.

Reading one row back then means unpacking the sixty four it sits with, which is only affordable
because nothing has to be searched for. The group holding a row is `(rowid / 64) * 64`, which is also
the group's key, so finding it is one seek; and the position of a blob inside the group is the sum of
the lengths recorded before it. Both are arithmetic on numbers already in hand, and unpacking a group
takes about six microseconds — less than fetching the row that asked for it.

The newest rows are left alone. A group is only worth making once it is complete, so packing stops a
group short of the end of the table and those rows are compressed one at a time until later runs
overtake them.

`/co compact` and the nightly maintenance run do this work, in batches, resuming where they left off
if interrupted. Groups whose rows have all been removed by retention are dropped with them.

### Where this shows up in `/co status`

`/co status` reports storage as one figure, since where a row currently sits makes no difference to
anyone, and breaks packed entity data out so it is clear what packing it saved:

```text
Storage: 2.29 GB holding 132,294,578 rows of activity.
Entity Data: 680.0 MB packed, saving 51.0 GB.
```

## The inspector

`/co i` reads packed storage as well as the rows still waiting to be packed, so clicking a block shows
its whole history.

That is affordable because the coordinates narrow it down: only the segments whose chunks could hold
the block are opened, inside them only the groups of rows around the block are decoded, and only the
rows at those coordinates are handed to the query. A click costs well under a millisecond per segment
it has to open.

Two of its views are keyed by an entity rather than by a place — the contents of an entity's
inventory, and interactions with an entity. They have no coordinates to go on, so each segment also
records which entities it holds rows for, and a lookup about one entity opens only the segments that
could hold something of it. Segments written before that was recorded gain it on the next
`/co compact`.

## Limits

* This layout is SQLite only. DuckDB and ClickHouse compress their own storage; MySQL is unchanged.
* A purge limited to one world or to specific block types only covers rows that have not been packed yet, since removing part of a segment would mean rewriting it. With activity packed minutes after it is logged, such a purge removes very little; purge by time instead.
* Entity data is never packed into a segment. It is read one row at a time by row id, so it stays in the live tables where a single row can be fetched without unpacking anything else; it is compressed where it lies instead, as described above.

## Upgrading an existing database

The layout is not compatible with databases written by upstream CoreProtect, and there is no conversion. On first start, an older database file in `storage/CoreProtect/` is renamed to `database.db.v1-<timestamp>` and a new, empty database takes its place. Renaming moves the file rather than copying it, so this needs no extra disk space and nothing is deleted — the old file stays where it is until you remove it yourself.

If you want your old history, keep running upstream CoreProtect against the renamed file, or delete it once you no longer need it.

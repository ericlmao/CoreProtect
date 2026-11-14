package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * The grouped segment format: what goes in comes back out, a lookup opens only the groups that can
 * hold what it wants, and skipping a group never loses a row it should have kept.
 */
class SegmentGroupsTest {

    private static final int[] TYPES = { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER,
            ColdSegmentCodec.TYPE_TEXT, ColdSegmentCodec.TYPE_BLOB, ColdSegmentCodec.TYPE_REAL };

    private static final int TIME = 0;
    private static final int X = 1;
    private static final int USER = 2;

    /** Rows with a place: time, world, x, z, user and a blob, as the location tests use them. */
    private static final String[] PLACED_COLUMNS = { "time", "wid", "x", "z", "user", "meta" };
    private static final int[] PLACED_TYPES = { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER,
            ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_BLOB };
    private static final int[] PLACED_LOCATION = { 1, 2, 3 };

    /**
     * A segment as the grouped format before chunk lists wrote it, with groups of sixteen rows,
     * produced by that build from the rows {@link #versionTwoRows()} describes.
     */
    private static final String VERSION_TWO_SCALARS = "Q1MCBQEBAQECyAENAggB1gb1AQHoA7oBEBETEhMUDAAB0A/uDwECBAABjwOGAwAQERMSEhUNFAHwD44QAQIEAAHpAvwCABARExIVFQ0qAZAQrhABAgQB5QL+"
                + "AgHzAvICABARExITFAxAAbAQzhABAgQAAf0C6AIAEBETEhUUDVQB0BDuEAECBAGFA94CAYcD3gIAEBETEhMUDWoB8BCOEQECBAAB4QKOAwAQERMSFRUMgAEB"
                + "kBGuEQECBAHbAogDAesChAMAEBETEhMVDZQBAbARzhEBAgQAAfUC+gIAEBETEhUVDaoBAdAR7hEBAgQB+wLoAgH/AvACABARExIUFQzAAQHwEY4SAQIEAAGJ"
                + "A9wCABARExIVFQ3UAQGQEq4SAQIEAY0DhAMB4wKMAwAQERMSFBUN6gEBsBLOEgECBAAB7QKCAwAICQoJCwoGgAIB0BLeEgECBAHxAuQCAb0C+AIAKLUv/WBW"
                + "Al0HAIKIHCSgpenrYMbp6h1np8iWQ8dX9ktV8EKdfnbW/uv/MmaBd1tLcgdtp6QtNCVQmcf3efhEPpVPDHj2ZBWf7EkjgY1CcN87bAwDV3psnCssbvUOQcwE"
                + "XAEuwK61+l14PAJTSW/AVF9EeqotzDkiqhlLq9MwCkOosMqM0wEggyGbDhJgAmEO5jiQg9BB1f3/DUw6Wcre5TFdz+pq+gUHlUYcx/z8lrdyYV+cZY+T4/4C"
                + "YmS/1kAH+8n8fl/YwS44M2xYOCkgUUM+tms2IjwOmcBcDie2mpCiN5MwMSrGFh6AhrPCmIEQA+15rwEotS/9YOgAhQUAYkYWILAnHYDkqFw9AFa1E28JWmFw"
                + "KHl65jgmDjMRZtYePkkZbdt2aaqWRhlOyadc+PhcZXxKudqejRSREbZHYAIZZQNV+cM77N1dny0KSvsS2A8YxxEcTiBxAEwqINCCqXQNicTXw6dglRicIHMM"
                + "eBS6IFvGwIMOKMA11SxfwGrjA9BmBS3LaW10sYAVJyKQiy+RE8eFFwaD4gWaIJq4M3tuFp4D+XIbRQR5JA/ZDAE=";
    private static final String VERSION_TWO_PAYLOAD = "bjFuMm40bjBuMm4zbjBuMW4zbjRuMW4ybjRuMG4ybjNuMG4xbjNuNG4xbjJuNG4wbjJuM24wbjFuM240bjFuMm40bjBuMm4zbjBuMW4zbjRuMW4ybjRuMG4y"
                + "bjNuMG4xbjNuNG4xbjJuNG4wbjJuM24wbjFuM240bjFuMm40bjBuMm4zbjBuMW4zbjRuMW4ybjRuMG4ybjNuMG4xbjNuNG4xbjJuNG4wbjJuM24wbjFuM240"
                + "bjFuMm40bjBuMm4zbjBuMW4zbjRuMW4ybjRuMG4ybjNuMG4xbjNuNG4xbjJuNG4wbjJuM24wbjFuM240bjFuMm40bjBuMm4zbjBuMW4zbjRuMW4ybjQ=";

    @Test
    void everyRowComesBackAcrossManyGroups() {
        List<Object[]> rows = sample(5000, 3);
        long[] rowIds = rowIds(rows.size());

        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds, rows);
        assertTrue(ColdSegmentCodec.isSelfContained(frames.getScalars()), "a grouped frame is stored as it is");

        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload());
        assertRowsEqual(rowIds, rows, decoded);
    }

    @Test
    void segmentsWrittenBeforeGroupsStillRead() {
        List<Object[]> rows = sample(3000, 5);
        long[] rowIds = rowIds(rows.size());

        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encodeVersionOne(TYPES, rowIds, rows);
        assertFalse(ColdSegmentCodec.isSelfContained(frames.getScalars()), "a first format frame is compressed as a whole when stored");

        assertRowsEqual(rowIds, rows, ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload()));
    }

    @Test
    void segmentsWrittenBeforeGroupsListedChunksStillRead() {
        byte[] scalars = Base64.getDecoder().decode(VERSION_TWO_SCALARS);
        byte[] payload = Base64.getDecoder().decode(VERSION_TWO_PAYLOAD);
        assertEquals(ColdSegmentCodec.VERSION_GROUPS, scalars[2], "the fixture is a frame of the grouped format before chunk lists");
        assertTrue(ColdSegmentCodec.isSelfContained(scalars), "a grouped frame of any version is stored as it is");

        List<Object[]> rows = versionTwoRows();
        long[] rowIds = new long[rows.size()];
        for (int row = 0; row < rowIds.length; row++) {
            rowIds[row] = 100 + 3L * row;
        }
        assertRowsEqual(rowIds, rows, ColdSegmentCodec.decode(scalars, payload));

        // Its groups list no chunks, so a filter is never asked about them and every row stays
        // reachable, however sure of itself the filter is.
        ColdSegmentCodec.RowFilter refusesEveryChunk = new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return true;
            }

            @Override
            public boolean mayMatchChunks(ColdSegmentCodec.ChunkList chunks) {
                throw new AssertionError("a frame without chunk lists was asked about its chunks");
            }
        };
        assertEquals(rows.size(), ColdSegmentCodec.count(scalars, refusesEveryChunk));
    }

    @Test
    void aGroupIsSkippedByTheChunksItListsWhereItsRangesCannotTell() {
        // Each group holds two chunks on opposite sides of the origin, so its ranges of x and z take
        // in everything between them, while the group really lies in two chunks only.
        List<Object[]> rows = diagonalGroups(16);
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds, rows, PLACED_LOCATION);
        assertEquals(ColdSegmentCodec.CODEC_VERSION, frames.getDirectory()[2]);

        // Chunk 3,3 lies inside the ranges of groups 3 to 15, and in the chunks of none of them.
        ColdSegmentCodec.RowFilter nowhere = placeFilter(16 * 3 + 5, 16 * 3 + 5);
        long before = ColdSegmentReadCounter.groupReads();
        assertEquals(0, ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), nowhere).size());
        assertEquals(0, ColdSegmentReadCounter.groupReads() - before, "no group lies in the chunk asked about");

        // Chunk 5,-5 lies in group 5 alone, though groups 5 to 15 all range over it.
        ColdSegmentCodec.RowFilter place = placeFilter(16 * 5 + 3, 16 * -5 + 3);
        before = ColdSegmentReadCounter.groupReads();
        ColdSegmentCodec.Rows found = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), place);
        assertEquals(1, ColdSegmentReadCounter.groupReads() - before, "only the group lying in the chunk is opened");

        ColdSegmentCodec.RowFilter rangesOnly = new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return place.accept(columns, present);
            }

            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                return place.mayMatch(minimum, maximum, known);
            }
        };
        before = ColdSegmentReadCounter.groupReads();
        assertSameRows(ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), rangesOnly), found);
        assertEquals(11, ColdSegmentReadCounter.groupReads() - before, "the ranges alone would open every group spanning the chunk");
        assertEquals(2, found.size(), "the two rows at that block");
    }

    @Test
    void aGroupHoldingARowWithoutAPlaceIsNeverSkipped() {
        List<Object[]> rows = diagonalGroups(16);
        // One row without an x in group 7, and one without a world in group 9.
        rows.get(7 * ColdSegmentCodec.GROUP_ROWS + 10)[2] = null;
        rows.get(9 * ColdSegmentCodec.GROUP_ROWS + 20)[1] = null;
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds, rows, PLACED_LOCATION);

        ColdSegmentCodec.RowFilter nowhere = placeFilter(16 * 3 + 5, 16 * 3 + 5);
        long before = ColdSegmentReadCounter.groupReads();
        ColdSegmentCodec.Rows found = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), nowhere);
        assertEquals(2, ColdSegmentReadCounter.groupReads() - before, "both groups holding a row without a place are opened");
        ColdSegmentCodec.RowFilter plain = nowhere::accept;
        assertSameRows(ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), plain), found);
        assertEquals(1, found.size(), "the row without an x is kept, as reading everything keeps it");
    }

    @Test
    void aGroupSpanningTooManyChunksListsNoneAndIsAlwaysRead() {
        // A group of chat or commands, ordered by player, can have a chunk to every row.
        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < ColdSegmentCodec.GROUP_ROWS; row++) {
            rows.add(new Object[] { 1000L + row, 1L, (long) (row * 16), (long) (row * 16), 1L, null });
        }
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds(rows.size()), rows, PLACED_LOCATION);

        long before = ColdSegmentReadCounter.groupReads();
        ColdSegmentCodec.Rows found = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), placeFilter(16 * 40 + 1, 16 * 41 + 1));
        assertEquals(1, ColdSegmentReadCounter.groupReads() - before, "a group without a list may hold any chunk");
        assertEquals(0, found.size());
        assertEquals(1, ColdSegmentCodec.count(frames.getScalars(), placeFilter(16 * 40, 16 * 40)));
    }

    @Test
    void theSegmentCountsEveryChunkExactly() {
        List<Object[]> rows = diagonalGroups(4);
        rows.get(5)[3] = null;
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds(rows.size()), rows, PLACED_LOCATION);
        SegmentChunkCounts counts = SegmentChunkCounts.decode(frames.getChunkCounts());

        assertEquals(1, counts.unlocatedRows());
        assertEquals(7, counts.distinctChunks(), "chunk 0,0 and two chunks for each other group");
        assertEquals(ColdSegmentCodec.GROUP_ROWS - 1, counts.rowsIn(1, 0, 0, 0, 0), "group 0 lies wholly in chunk 0,0, less its row without a z");
        assertEquals(ColdSegmentCodec.GROUP_ROWS / 2, counts.rowsIn(1, 2, 2, -2, -2));
        assertEquals(ColdSegmentCodec.GROUP_ROWS * 3L, counts.rowsIn(1, -3, 3, -3, 3) - counts.rowsIn(1, 0, 0, 0, 0));
        assertEquals(0, counts.rowsIn(1, 1, 1, 1, 1));
        assertEquals(0, counts.rowsIn(2, 0, 0, 0, 0), "another world holds nothing");
        assertTrue(counts.mayHoldAny(1, 1, 1, 1, 1), "a segment with a row without a place may hold anything");
    }

    @Test
    void countsStayExactForChunksFarApartInManyWorlds() {
        // Ten worlds and chunks thousands apart, too many and too far to pack a chunk into an int,
        // and one chunk holding more rows than a byte can count.
        List<Object[]> rows = new ArrayList<>();
        for (int world = 1; world <= 10; world++) {
            rows.add(new Object[] { 1L, (long) world, 16L * 20_000, -16L * 20_000, 1L, null });
            rows.add(new Object[] { 1L, (long) world, -16L * 30_000 + 7, 80L, 1L, null });
        }
        for (int row = 0; row < 300; row++) {
            rows.add(new Object[] { 1L, 4L, (long) row % 16, 0L, 1L, null });
        }
        SegmentChunkCounts counts = SegmentChunkCounts.decode(ColdSegmentCodec.encode(PLACED_TYPES, rowIds(rows.size()), rows, PLACED_LOCATION).getChunkCounts());

        assertEquals(21, counts.distinctChunks());
        assertEquals(300, counts.rowsIn(4, 0, 0, 0, 0), "a chunk holding more rows than a byte counts");
        assertEquals(301, counts.rowsIn(4, -40_000, 0, 0, 10), "a rectangle spanning many columns");
        assertEquals(1, counts.rowsIn(7, 20_000, 20_000, -20_000, -20_000));
        assertEquals(1, counts.rowsIn(10, -30_000, -30_000, 5, 5));
        assertEquals(0, counts.rowsIn(10, -30_000, -30_000, 4, 4));
        assertEquals(0, counts.rowsIn(11, -40_000, 40_000, -40_000, 40_000), "a world the segment has no rows in");
        assertFalse(counts.mayHoldAny(3, 1, 1, 1, 1));
        assertTrue(counts.mayHoldAny(3, 19_999, 20_001, -20_001, -19_999));
    }

    @Test
    void aNarrowLookupOpensOnlyTheGroupsThatCanHoldIt() {
        // Rows ordered by x, the way packing orders them by place, so each group covers a band of x.
        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < 16 * ColdSegmentCodec.GROUP_ROWS; row++) {
            rows.add(new Object[] { 1000L + row, (long) row, (long) (row % 9), "t", null, 1.5 });
        }
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds, rows);

        long target = 5 * ColdSegmentCodec.GROUP_ROWS + 17;
        ColdSegmentCodec.RowFilter filter = rangeFilter(X, target, target);

        long before = ColdSegmentReadCounter.groupReads();
        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), filter);
        assertEquals(1, ColdSegmentReadCounter.groupReads() - before, "one group of sixteen is opened");
        assertEquals(1, decoded.size());
        assertEquals(rowIds[(int) target], decoded.getRowId(0));
    }

    @Test
    void aGroupWithEmptyValuesIsNeverSkippedOnThatColumn() {
        // A row with no value in a column is accepted by any test on that column, so a group holding
        // one cannot be ruled out by the range of the others.
        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < 3 * ColdSegmentCodec.GROUP_ROWS; row++) {
            Long x = row == ColdSegmentCodec.GROUP_ROWS + 3 ? null : Long.valueOf(row);
            rows.add(new Object[] { 1000L + row, x, 1L, null, null, null });
        }
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds, rows);

        ColdSegmentCodec.RowFilter filter = rangeFilter(X, 10, 10);
        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), filter);

        List<Long> kept = new ArrayList<>();
        for (int row = 0; row < decoded.size(); row++) {
            kept.add(decoded.getRowId(row));
        }
        assertTrue(kept.contains(rowIds[10]), "the matching row is kept");
        assertTrue(kept.contains(rowIds[ColdSegmentCodec.GROUP_ROWS + 3]), "the row without a value is kept, as a full read keeps it");
        assertEquals(2, kept.size());
    }

    @Test
    void skippingGroupsGivesExactlyWhatReadingEverythingGives() {
        Random random = new Random(9);
        List<Object[]> rows = sample(20000, 11);
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds, rows);

        for (int attempt = 0; attempt < 50; attempt++) {
            long low = random.nextInt(20000);
            long high = low + random.nextInt(400);
            long user = random.nextInt(12);
            ColdSegmentCodec.RowFilter pruning = new ColdSegmentCodec.RowFilter() {
                @Override
                public boolean accept(long[] columns, boolean[] present) {
                    return (!present[TIME] || (columns[TIME] >= low && columns[TIME] <= high)) && (!present[USER] || columns[USER] == user);
                }

                @Override
                public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                    return !(known[TIME] && (maximum[TIME] < low || minimum[TIME] > high)) && !(known[USER] && (user < minimum[USER] || user > maximum[USER]));
                }
            };
            ColdSegmentCodec.RowFilter plain = pruning::accept;

            ColdSegmentCodec.Rows pruned = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), pruning);
            ColdSegmentCodec.Rows full = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), plain);
            assertEquals(full.size(), pruned.size(), "attempt " + attempt);
            for (int row = 0; row < full.size(); row++) {
                assertEquals(full.getRowId(row), pruned.getRowId(row));
            }
            assertEquals(full.size(), ColdSegmentCodec.count(frames.getScalars(), pruning));
        }
    }

    @Test
    void countingNeverTouchesThePayload() {
        List<Object[]> rows = sample(4000, 2);
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds(rows.size()), rows);
        ColdSegmentCodec.Payload refuse = () -> {
            throw new AssertionError("a count asked for the payload");
        };

        assertEquals(4000, ColdSegmentCodec.count(frames.getScalars(), null));
        // Building only rows whose text and blob columns are empty does not need it either.
        ColdSegmentCodec.RowFilter noText = (columns, present) -> !present[3] && !present[4];
        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(frames.getScalars(), refuse, noText);
        assertTrue(decoded.size() > 0);
    }

    @Test
    void thePayloadIsReadOnceWhenARowNeedsIt() {
        List<Object[]> rows = sample(4000, 2);
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(TYPES, rowIds(rows.size()), rows);
        int[] asked = new int[1];
        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(frames.getScalars(), () -> {
            asked[0]++;
            return frames.getPayload();
        }, null);
        assertEquals(4000, decoded.size());
        assertEquals(1, asked[0]);
    }

    @Test
    void aSplitFrameDecodesFromTheUnitsItsFilterNeedsAlone() {
        // Twenty four groups in three units of eight. Chunk 13,-13 lies in group 13 alone, in the
        // second unit, though the ranges of every group from 13 on take it in.
        List<Object[]> rows = diagonalGroups(24);
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds, rows, PLACED_LOCATION);
        byte[] directory = frames.getDirectory();
        assertTrue(ColdSegmentCodec.isSplit(directory), "the directory is stored without the unit bodies");
        assertTrue(ColdSegmentCodec.isSelfContained(directory), "and as it is, never compressed as a whole");
        assertEquals(3, frames.getUnits().length);
        assertEquals(3, ColdSegmentCodec.unitCount(directory));

        ColdSegmentCodec.RowFilter place = placeFilter(16 * 13 + 3, 16 * -13 + 3);
        assertArrayEquals(new int[] { 1 }, ColdSegmentCodec.neededUnits(directory, place), "only the unit holding the group is needed");
        byte[][] needed = new byte[3][];
        needed[1] = frames.getUnits()[1];
        ColdSegmentCodec.Rows found = ColdSegmentCodec.decode(directory, needed, frames.getPayload(), place);
        assertTrue(found.size() > 0);
        assertSameRows(ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload(), place), found);
        assertEquals(found.size(), ColdSegmentCodec.count(directory, needed, place));

        assertArrayEquals(new int[] { 0, 1, 2 }, ColdSegmentCodec.neededUnits(directory, null), "reading everything needs every unit");
        assertRowsEqual(rowIds, rows, ColdSegmentCodec.decode(directory, frames.getUnits(), frames.getPayload(), null));

        // A unit holding rows the filter wants that was not read is an error rather than a gap.
        assertThrows(IllegalArgumentException.class, () -> ColdSegmentCodec.decode(directory, new byte[3][], frames.getPayload(), place));
        byte[][] truncated = new byte[3][];
        truncated[1] = java.util.Arrays.copyOf(frames.getUnits()[1], frames.getUnits()[1].length - 1);
        assertThrows(IllegalArgumentException.class, () -> ColdSegmentCodec.decode(directory, truncated, frames.getPayload(), place), "a body its directory does not describe is refused");
    }

    @Test
    void aFrameWithItsBodiesInLineSplitsIntoTheSameDirectoryAndUnits() {
        List<Object[]> rows = diagonalGroups(20);
        long[] rowIds = rowIds(rows.size());
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PLACED_TYPES, rowIds, rows, PLACED_LOCATION);
        byte[] inline = frames.getScalars();
        assertEquals(ColdSegmentCodec.VERSION_CHUNKS, inline[2], "joined, the frame is exactly what version 3 stored");
        assertFalse(ColdSegmentCodec.isSplit(inline));
        assertRowsEqual(rowIds, rows, ColdSegmentCodec.decode(inline, frames.getPayload()));

        ColdSegmentCodec.Frames split = ColdSegmentCodec.split(inline);
        assertArrayEquals(frames.getDirectory(), split.getDirectory());
        assertEquals(frames.getUnits().length, split.getUnits().length);
        for (int unit = 0; unit < frames.getUnits().length; unit++) {
            assertArrayEquals(frames.getUnits()[unit], split.getUnits()[unit], "unit " + unit + " is moved byte for byte");
        }
        assertArrayEquals(inline, ColdSegmentCodec.inline(split.getDirectory(), split.getUnits()));
        assertThrows(IllegalArgumentException.class, () -> ColdSegmentCodec.split(frames.getDirectory()), "a directory has nothing left to split");
    }

    /**
     * The filter a lookup about one block of world 1 reads a segment with, exactly as the index
     * builds it.
     */
    private static ColdSegmentCodec.RowFilter placeFilter(int x, int z) {
        SQLiteColdIndex.TableLayout layout = new SQLiteColdIndex.TableLayout(PLACED_COLUMNS, PLACED_TYPES, new String[] { "INTEGER", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "BLOB" });
        return SQLiteColdIndex.rowFilter(layout, 1, new Integer[] { 0, x, x, null, null, z, z }, 0, 0, null, null, null, SQLiteColdIndex.Exclusions.NONE);
    }

    /**
     * Groups whose rows lie half in chunk g,-g and half in chunk -g,g, so every group's ranges of x
     * and z span from one to the other while the group lies in two chunks.
     */
    private static List<Object[]> diagonalGroups(int groups) {
        List<Object[]> rows = new ArrayList<>();
        for (int group = 0; group < groups; group++) {
            for (int row = 0; row < ColdSegmentCodec.GROUP_ROWS; row++) {
                int chunkX = row % 2 == 0 ? group : -group;
                int chunkZ = -chunkX;
                long x = 16L * chunkX + ((row / 2) % 16);
                long z = 16L * chunkZ + ((row / 32) % 16);
                rows.add(new Object[] { 1000L + rows.size(), 1L, x, z, (long) (row % 7), row % 5 == 0 ? new byte[] { (byte) row } : null });
            }
        }
        return rows;
    }

    /** The rows the version two fixture was made from. */
    private static List<Object[]> versionTwoRows() {
        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < 200; row++) {
            Long x = row % 31 == 0 ? null : Long.valueOf((row * 37 % 400) - 200);
            String note = row % 3 == 0 ? null : "n" + (row % 5);
            rows.add(new Object[] { 1000L + row, (long) (1 + (row % 7 == 0 ? 1 : 0)), x, (long) ((row * 53 % 400) - 200), note });
        }
        return rows;
    }

    private static void assertSameRows(ColdSegmentCodec.Rows expected, ColdSegmentCodec.Rows actual) {
        assertEquals(expected.size(), actual.size());
        for (int row = 0; row < expected.size(); row++) {
            assertEquals(expected.getRowId(row), actual.getRowId(row));
        }
    }

    private static ColdSegmentCodec.RowFilter rangeFilter(int column, long low, long high) {
        return new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return !present[column] || (columns[column] >= low && columns[column] <= high);
            }

            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                return !known[column] || (maximum[column] >= low && minimum[column] <= high);
            }
        };
    }

    private static List<Object[]> sample(int count, long seed) {
        Random random = new Random(seed);
        List<Object[]> rows = new ArrayList<>(count);
        for (int row = 0; row < count; row++) {
            Long x = random.nextInt(50) == 0 ? null : Long.valueOf(random.nextInt(4000) - 2000);
            String text = random.nextInt(3) == 0 ? null : "text " + random.nextInt(100);
            byte[] blob = random.nextInt(4) == 0 ? null : ("blob-" + row).getBytes(StandardCharsets.UTF_8);
            Double real = random.nextBoolean() ? null : Double.valueOf(random.nextDouble() * 100);
            rows.add(new Object[] { (long) row, x, (long) random.nextInt(12), text, blob, real });
        }
        return rows;
    }

    private static long[] rowIds(int count) {
        long[] rowIds = new long[count];
        for (int row = 0; row < count; row++) {
            rowIds[row] = 100 + row * 3L;
        }
        return rowIds;
    }

    private static void assertRowsEqual(long[] rowIds, List<Object[]> rows, ColdSegmentCodec.Rows decoded) {
        assertEquals(rows.size(), decoded.size());
        for (int row = 0; row < rows.size(); row++) {
            assertEquals(rowIds[row], decoded.getRowId(row));
            Object[] expected = rows.get(row);
            Object[] actual = decoded.getValues(row);
            for (int column = 0; column < expected.length; column++) {
                if (expected[column] instanceof byte[]) {
                    assertArrayEquals((byte[]) expected[column], (byte[]) actual[column], "row " + row + " column " + column);
                }
                else {
                    assertEquals(expected[column], actual[column], "row " + row + " column " + column);
                }
            }
        }
    }
}

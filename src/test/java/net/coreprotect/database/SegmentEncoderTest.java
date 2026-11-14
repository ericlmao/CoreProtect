package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * Compaction writes segments a group of rows at a time and reads them back the same way, keeping
 * each unit's share of the payload apart. Written that way a segment must be exactly the segment
 * the whole-segment encoder writes, and read that way it must give back exactly the rows the
 * whole-segment decoder does.
 */
class SegmentEncoderTest {

    /** Time, user, world, x, y and z, then a text, a blob and a real column. */
    private static final int[] TYPES = { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER,
            ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT, ColdSegmentCodec.TYPE_BLOB, ColdSegmentCodec.TYPE_REAL };

    /** The world, x and z columns. */
    private static final int[] LOCATION = { 2, 3, 5 };

    @Test
    void writingAGroupAtATimeGivesExactlyTheSegmentWrittenWhole() throws Exception {
        for (int rows : new int[] { 1, 1023, 1024, 1025, 8192, 8193, 20_000 }) {
            long[] rowIds = rowIds(rows, 11);
            List<Object[]> values = rows(rows, 11);
            ColdSegmentCodec.Frames whole = ColdSegmentCodec.encode(TYPES, rowIds, values, LOCATION);
            ColdSegmentCodec.Encoded streamed = streamed(rowIds, values, null);

            assertArrayEquals(whole.getDirectory(), streamed.getDirectory(), "the directory of " + rows + " rows");
            assertTrue(Arrays.deepEquals(whole.getUnits(), streamed.getUnits()), "the unit bodies of " + rows + " rows");
            assertArrayEquals(whole.getPayload(), joined(streamed.getUnitPayloads()), "the shares joined are the payload frame, for " + rows + " rows");
            assertEquals(whole.getPayload().length, streamed.getPayloadSize());
            assertArrayEquals(whole.getChunkCounts(), streamed.getChunkCounts(), "the rows per chunk of " + rows + " rows");
        }
    }

    @Test
    void onlyTheLastGroupMayBeShort() throws Exception {
        ColdSegmentCodec.Encoder encoder = new ColdSegmentCodec.Encoder(TYPES, LOCATION, null);
        encoder.addGroup(rowIds(10, 3), rows(10, 3), 10);
        assertThrows(IllegalStateException.class, () -> encoder.addGroup(rowIds(10, 4), rows(10, 4), 10));
    }

    @Test
    void readingAGroupAtATimeGivesBackEveryRowWithItsTextAndBlobs() throws Exception {
        int rows = 20_000;
        long[] rowIds = rowIds(rows, 17);
        List<Object[]> values = rows(rows, 17);
        ColdSegmentCodec.Encoded encoded = streamed(rowIds, values, null);
        ColdSegmentCodec.Rows expected = ColdSegmentCodec.decode(encoded.getDirectory(), encoded.getUnits(), joined(encoded.getUnitPayloads()), null);

        ColdSegmentCodec.GroupReader reader = new ColdSegmentCodec.GroupReader(encoded.getDirectory());
        assertEquals(rows, reader.rowCount());
        long[] ids = new long[ColdSegmentCodec.GROUP_ROWS];
        Object[][] built = new Object[ColdSegmentCodec.GROUP_ROWS][];
        int row = 0;
        for (int unit = 0; unit < reader.unitCount(); unit++) {
            byte[] body = reader.openUnit(unit, encoded.getUnits()[unit]);
            byte[] share = encoded.getUnitPayloads()[unit];
            assertEquals(reader.payloadEnd(unit, encoded.getPayloadSize()) - reader.payloadStart(unit), share.length, "each share is the unit's stretch of the payload");
            for (int group = reader.firstGroup(unit); group < reader.endGroup(unit); group++) {
                int count = reader.decodeGroup(group, body, share, reader.payloadStart(unit), ids, built);
                for (int index = 0; index < count; index++, row++) {
                    assertEquals(expected.getRowId(row), ids[index]);
                    assertTrue(Arrays.deepEquals(expected.getValues(row), built[index]), "row " + row);
                }
            }
        }
        assertEquals(rows, row);
    }

    @Test
    void aLookupReadsOnlyTheSharesHoldingTheValuesItKeeps() throws Exception {
        int rows = 30_000;
        long[] rowIds = rowIds(rows, 23);
        List<Object[]> values = rows(rows, 23);
        ColdSegmentCodec.Encoded encoded = streamed(rowIds, values, null);
        byte[] payload = joined(encoded.getUnitPayloads());
        int[] starts = ColdSegmentCodec.unitPayloadStarts(encoded.getDirectory());
        assertEquals(encoded.getUnits().length, starts.length);

        // The rows of one place, which lie in one unit only, as rows ordered by place do.
        ColdSegmentCodec.RowFilter onePlace = (columns, present) -> present[3] && columns[3] >= 640 && columns[3] < 656;
        ColdSegmentCodec.Selection selection = ColdSegmentCodec.select(encoded.getDirectory(), encoded.getUnits(), onePlace);
        assertTrue(selection.needsPayload());
        int[] wanted = selection.payloadUnits(starts);
        assertTrue(wanted.length > 0 && wanted.length < starts.length, "the kept values lie in " + wanted.length + " of " + starts.length + " units");

        byte[][] shares = new byte[starts.length][];
        for (int unit : wanted) {
            shares[unit] = encoded.getUnitPayloads()[unit];
        }
        ColdSegmentCodec.Rows fromShares = selection.buildFromShares(new ColdSegmentCodec.PayloadShares(starts, shares));
        ColdSegmentCodec.Rows fromFrame = ColdSegmentCodec.select(encoded.getDirectory(), encoded.getUnits(), onePlace).build(payload);
        assertEquals(fromFrame.size(), fromShares.size());
        for (int row = 0; row < fromFrame.size(); row++) {
            assertEquals(fromFrame.getRowId(row), fromShares.getRowId(row));
            assertTrue(Arrays.deepEquals(fromFrame.getValues(row), fromShares.getValues(row)), "row " + row);
        }
    }

    @Test
    void sharesArePackedAsEachUnitIsFinished() throws Exception {
        int rows = 20_000;
        List<Integer> packed = new ArrayList<>();
        ColdSegmentCodec.Encoded encoded = streamed(rowIds(rows, 5), rows(rows, 5), raw -> {
            packed.add(raw.length);
            return com.github.luben.zstd.Zstd.compress(raw, 3);
        });
        assertEquals(encoded.getUnits().length, packed.size(), "one share per unit");
        long total = 0;
        for (int unit = 0; unit < packed.size(); unit++) {
            assertEquals(packed.get(unit).intValue(), com.github.luben.zstd.Zstd.decompress(encoded.getUnitPayloads()[unit], packed.get(unit)).length);
            total = total + packed.get(unit);
        }
        assertEquals(encoded.getPayloadSize(), total);
    }

    private static ColdSegmentCodec.Encoded streamed(long[] rowIds, List<Object[]> values, ColdSegmentCodec.PayloadPacker packer) throws Exception {
        ColdSegmentCodec.Encoder encoder = new ColdSegmentCodec.Encoder(TYPES, LOCATION, packer);
        for (int from = 0; from < rowIds.length; from += ColdSegmentCodec.GROUP_ROWS) {
            int count = Math.min(ColdSegmentCodec.GROUP_ROWS, rowIds.length - from);
            encoder.addGroup(Arrays.copyOfRange(rowIds, from, from + count), new ArrayList<>(values.subList(from, from + count)), count);
        }
        return encoder.finish();
    }

    private static byte[] joined(byte[][] shares) {
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        for (byte[] share : shares) {
            joined.write(share, 0, share.length);
        }
        return joined.toByteArray();
    }

    private static long[] rowIds(int rows, long seed) {
        Random random = new Random(seed);
        long[] ids = new long[rows];
        long id = 1000;
        for (int row = 0; row < rows; row++) {
            id = id + 1 + random.nextInt(3);
            ids[row] = id;
        }
        return ids;
    }

    /** Rows ordered by place, with values missing here and there and text and blobs of every size. */
    private static List<Object[]> rows(int rows, long seed) {
        Random random = new Random(seed * 31);
        List<Object[]> values = new ArrayList<>(rows);
        for (int row = 0; row < rows; row++) {
            int area = row / 300;
            Object text = random.nextInt(6) == 0 ? null : (random.nextInt(9) == 0 ? "" : "said " + random.nextInt(100_000) + " at " + area);
            Object blob = random.nextInt(4) == 0 ? null : (random.nextInt(9) == 0 ? new byte[0] : randomBytes(random, 1 + random.nextInt(90)));
            values.add(new Object[] { 1_700_000_000L + row, (long) (1 + random.nextInt(8)), random.nextInt(50) == 0 ? null : 1L, (long) (area * 16 + random.nextInt(16)),
                    (long) (60 + random.nextInt(20)), (long) (area * 8 + random.nextInt(16)), text, blob, random.nextInt(3) == 0 ? null : random.nextDouble() * 64 });
        }
        return values;
    }

    private static byte[] randomBytes(Random random, int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}

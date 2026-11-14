package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * A segment packed before its table gained a column is read as if it had the column, holding the
 * table's default in every row, by every way the codec reads a segment.
 */
class SegmentAddedColumnTest {

    /** Time, world, x, z and a text column, as the segment was packed. */
    private static final int[] PACKED = { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER,
            ColdSegmentCodec.TYPE_TEXT };

    /** The same table since: an integer column defaulting to 0, a text one without a default, a real one defaulting to 1.5. */
    private static final ColdSegmentCodec.Columns NOW = new ColdSegmentCodec.Columns(
            new int[] { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT,
                    ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT, ColdSegmentCodec.TYPE_REAL },
            new Object[] { null, null, null, null, null, 0L, null, 1.5D });

    private static final int ROLLED_BACK = 5;
    private static final int ROWS = 5000;

    /** Accepts rows whose added integer column holds a value, and rules groups out by its range. */
    private static ColdSegmentCodec.RowFilter rolledBack(long wanted) {
        return new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return present[ROLLED_BACK] && columns[ROLLED_BACK] == wanted;
            }

            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                return !known[ROLLED_BACK] || (minimum[ROLLED_BACK] <= wanted && maximum[ROLLED_BACK] >= wanted);
            }
        };
    }

    @Test
    void everyReadShowsTheAddedColumnsWithTheirDefaults() {
        long[] rowIds = new long[ROWS];
        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            rowIds[row] = row + 1;
            rows.add(new Object[] { 1000L + row, 1L, (long) (row % 40), (long) (row / 40), "line " + row });
        }
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PACKED, rowIds, rows, new int[] { 1, 2, 3 });
        byte[] directory = frames.getDirectory();
        byte[][] units = frames.getUnits();

        ColdSegmentCodec.Rows decoded = ColdSegmentCodec.decode(directory, units, frames.getPayload(), null, NOW);
        assertEquals(ROWS, decoded.size());
        for (int row = 0; row < ROWS; row++) {
            Object[] values = decoded.getValues(row);
            assertEquals(8, values.length);
            assertEquals("line " + row, values[4]);
            assertEquals(0L, values[ROLLED_BACK], "an added integer column holds its default");
            assertNull(values[6], "an added column without a default holds nothing");
            assertEquals(1.5D, values[7]);
        }

        assertEquals(ROWS, ColdSegmentCodec.count(directory, units, rolledBack(0), NOW), "a filter on the added column sees its default");
        assertEquals(0, ColdSegmentCodec.count(directory, units, rolledBack(1), NOW));
        assertEquals(0, ColdSegmentCodec.neededUnits(directory, rolledBack(1), NOW).length, "the groups' ranges rule out a value the column cannot hold");
        assertEquals(units.length, ColdSegmentCodec.neededUnits(directory, rolledBack(0), NOW).length);
        assertEquals(ROWS, ColdSegmentCodec.keys(directory, units, rolledBack(0), 0, NOW).size());
        assertEquals(1000L, ColdSegmentCodec.keys(directory, units, rolledBack(0), 0, NOW).time(0));

        // Read a group at a time, as compaction reads it.
        ColdSegmentCodec.GroupReader reader = new ColdSegmentCodec.GroupReader(directory, NOW);
        assertEquals(8, reader.columnCount());
        long[] ids = new long[ColdSegmentCodec.GROUP_ROWS];
        Object[][] built = new Object[ColdSegmentCodec.GROUP_ROWS][];
        int row = 0;
        for (int unit = 0; unit < reader.unitCount(); unit++) {
            byte[] body = reader.openUnit(unit, units[unit]);
            for (int group = reader.firstGroup(unit); group < reader.endGroup(unit); group++) {
                int count = reader.decodeGroup(group, body, frames.getPayload(), 0, ids, built);
                for (int index = 0; index < count; index++, row++) {
                    assertEquals(rowIds[row], ids[index]);
                    assertArrayEquals(decoded.getValues(row), built[index], "row " + row);
                }
            }
        }
        assertEquals(ROWS, row);

        // The first segment format, as old databases still hold it.
        ColdSegmentCodec.Frames first = ColdSegmentCodec.encodeVersionOne(PACKED, rowIds, rows);
        ColdSegmentCodec.Rows old = ColdSegmentCodec.decode(first.getScalars(), null, first.getPayload(), rolledBack(0), NOW);
        assertEquals(ROWS, old.size());
        assertArrayEquals(decoded.getValues(17), old.getValues(17));
        assertEquals(0, ColdSegmentCodec.count(first.getScalars(), null, rolledBack(1), NOW));

        // Without the table's columns a segment reads as it was packed.
        assertEquals(PACKED.length, ColdSegmentCodec.decode(directory, units, frames.getPayload(), null).getValues(0).length);
    }

    @Test
    void aSegmentThatDoesNotMatchItsTableIsRefused() {
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(PACKED, new long[] { 1 }, List.<Object[]>of(new Object[] { 1L, 1L, 1L, 1L, "a" }));
        ColdSegmentCodec.Columns moved = new ColdSegmentCodec.Columns(
                new int[] { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT,
                        ColdSegmentCodec.TYPE_INTEGER },
                new Object[] { null, null, null, null, null, 0L });
        assertThrows(IllegalArgumentException.class, () -> ColdSegmentCodec.decode(frames.getDirectory(), frames.getUnits(), frames.getPayload(), null, moved));

        ColdSegmentCodec.Columns unknown = new ColdSegmentCodec.Columns(
                new int[] { ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_INTEGER, ColdSegmentCodec.TYPE_TEXT,
                        ColdSegmentCodec.TYPE_TEXT },
                new Object[] { null, null, null, null, null, ColdSegmentCodec.Columns.UNSUPPORTED_DEFAULT });
        assertThrows(IllegalArgumentException.class, () -> ColdSegmentCodec.decode(frames.getDirectory(), frames.getUnits(), frames.getPayload(), null, unknown));
    }

    @Test
    void columnDefaultsAreReadAsSQLiteReportsThem() {
        assertEquals(0L, SQLiteColdIndex.defaultValue("0", ColdSegmentCodec.TYPE_INTEGER));
        assertEquals(0L, SQLiteColdIndex.defaultValue("'0'", ColdSegmentCodec.TYPE_INTEGER));
        assertEquals(-3L, SQLiteColdIndex.defaultValue("(-3)", ColdSegmentCodec.TYPE_INTEGER));
        assertEquals(2.5D, SQLiteColdIndex.defaultValue("2.5", ColdSegmentCodec.TYPE_REAL));
        assertNull(SQLiteColdIndex.defaultValue(null, ColdSegmentCodec.TYPE_INTEGER));
        assertNull(SQLiteColdIndex.defaultValue("NULL", ColdSegmentCodec.TYPE_TEXT));
        assertEquals(ColdSegmentCodec.Columns.UNSUPPORTED_DEFAULT, SQLiteColdIndex.defaultValue("'text'", ColdSegmentCodec.TYPE_TEXT));
        assertEquals(ColdSegmentCodec.Columns.UNSUPPORTED_DEFAULT, SQLiteColdIndex.defaultValue("CURRENT_TIMESTAMP", ColdSegmentCodec.TYPE_INTEGER));
    }
}

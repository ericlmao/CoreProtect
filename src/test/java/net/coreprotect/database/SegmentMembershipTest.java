package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The gate for what a segment records about the values it holds.
 *
 * <p>
 * These records exist so a lookup can skip segments that cannot hold what it is asking for. Saying
 * "might hold it" when a segment does not costs one wasted read; saying "does not hold it" when the
 * segment does loses rows from the answer, silently. So the tests that matter here are the ones about
 * never doing the second.
 * </p>
 */
class SegmentMembershipTest {

    @Test
    void aFewValuesAreListedExactlyAndAnsweredWithCertainty() {
        long[] values = { 4, 17, 900, -3 };
        SegmentMembership membership = SegmentMembership.decode(SegmentMembership.encode(values));

        for (long value : values) {
            assertTrue(membership.mightContain(value), value + " was recorded");
        }
        // An exact list can rule things out, which a filter can only do most of the time.
        assertFalse(membership.mightContain(5), "5 was not recorded");
        assertFalse(membership.mightContain(0), "0 was not recorded");
    }

    @Test
    void nothingRecordedIsNothingStored() {
        assertNull(SegmentMembership.encode(new long[0]));
        assertNull(SegmentMembership.encode(null));
        assertNull(SegmentMembership.decode(null));
    }

    @Test
    void everyValueIsStillFoundOnceThereAreTooManyToList() {
        // Past the point where they are listed, the record becomes a filter. A filter may claim to
        // hold something it does not; it may never deny something it does.
        long[] values = new long[SegmentMembership.MAXIMUM_EXACT_VALUES * 4];
        for (int index = 0; index < values.length; index++) {
            values[index] = index * 7919L;
        }

        SegmentMembership membership = SegmentMembership.decode(SegmentMembership.encode(values, SegmentFilter.CHUNK_BYTES));
        for (long value : values) {
            assertTrue(membership.mightContain(value), value + " must never be denied");
        }
    }

    @Test
    void aLargerFilterIsUsedWhenAskedFor() {
        long[] values = new long[SegmentMembership.MAXIMUM_EXACT_VALUES + 1];
        for (int index = 0; index < values.length; index++) {
            values[index] = index;
        }

        // Chunks are far more numerous than players or block types, so their filter is larger. A
        // filter too small for what goes in it is wrong far more often than it needs to be.
        assertEquals(SegmentFilter.SMALL_BYTES + 1, SegmentMembership.encode(values).length);
        assertEquals(SegmentFilter.CHUNK_BYTES + 1, SegmentMembership.encode(values, SegmentFilter.CHUNK_BYTES).length);
    }

    @Test
    void listingIsMuchSmallerThanFilteringForTheFewValuesASegmentUsuallyHolds() {
        // This is the point of listing them: a segment covering a few dozen chunks used to spend four
        // kilobytes saying so.
        long[] chunks = new long[40];
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] = SegmentFilter.chunkKey(1, index, index * 3);
        }

        int listed = SegmentMembership.encode(chunks, SegmentFilter.CHUNK_BYTES).length;
        assertTrue(listed * 10 < SegmentFilter.CHUNK_BYTES, "listing 40 chunks takes " + listed + " bytes");
    }

    @Test
    void recordsWrittenBeforeThisFormatStillRead() {
        // Segments written earlier carried a bare filter with no tag byte, in one of two sizes.
        for (int size : new int[] { SegmentFilter.SMALL_BYTES, SegmentFilter.CHUNK_BYTES }) {
            SegmentFilter legacy = new SegmentFilter(size);
            legacy.add(12345);
            SegmentMembership membership = SegmentMembership.decode(legacy.toBytes());
            assertTrue(membership.mightContain(12345), "a filter of " + size + " bytes still reads");
        }
    }
}

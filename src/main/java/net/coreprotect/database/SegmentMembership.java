package net.coreprotect.database;

import java.util.Arrays;

/**
 * Records which values of a column appear in a segment, so a lookup can skip segments that cannot
 * contain what it is asking for.
 *
 * <p>
 * A segment usually sees only a handful of distinct players or block types, and for those the exact
 * set is both smaller than a filter and free of false positives. When a segment does see many
 * distinct values the set is replaced by a Bloom filter, which stays a fixed size no matter how many
 * values it holds. Both forms answer the same question, and both only ever err by saying "maybe"
 * about a value that is absent, never by hiding one that is present.
 * </p>
 */
public final class SegmentMembership {

    /** Distinct values kept as an exact list before falling back to a filter. */
    static final int MAXIMUM_EXACT_VALUES = 512;

    private static final int TAG_EXACT = 0;
    private static final int TAG_FILTER = 1;

    private final long[] values;
    private final SegmentFilter filter;

    private SegmentMembership(long[] values, SegmentFilter filter) {
        this.values = values;
        this.filter = filter;
    }

    /**
     * Encodes the distinct values of a column in one segment.
     *
     * @param values
     *            the distinct values seen, may be empty
     * @return the stored form, or null when nothing was seen
     */
    public static byte[] encode(long[] values) {
        return encode(values, SegmentFilter.SMALL_BYTES);
    }

    /**
     * Encodes the distinct values of a column in one segment, choosing the filter size for how many
     * of them there can be.
     *
     * @param values
     *            the distinct values seen, may be empty
     * @param filterBytes
     *            the size of the filter to fall back to when there are too many to list
     * @return the stored form, or null when nothing was seen
     */
    public static byte[] encode(long[] values, int filterBytes) {
        if (values == null || values.length == 0) {
            return null;
        }

        if (values.length <= MAXIMUM_EXACT_VALUES) {
            long[] sorted = Arrays.copyOf(values, values.length);
            Arrays.sort(sorted);
            byte[] encoded = new byte[1 + (sorted.length * 8)];
            encoded[0] = TAG_EXACT;
            int offset = 1;
            for (long value : sorted) {
                for (int shift = 56; shift >= 0; shift -= 8) {
                    encoded[offset++] = (byte) (value >>> shift);
                }
            }
            return encoded;
        }

        SegmentFilter bloom = new SegmentFilter(filterBytes);
        for (long value : values) {
            bloom.add(value);
        }
        byte[] bytes = bloom.toBytes();
        byte[] encoded = new byte[1 + bytes.length];
        encoded[0] = TAG_FILTER;
        System.arraycopy(bytes, 0, encoded, 1, bytes.length);
        return encoded;
    }

    /**
     * @param encoded
     *            the stored form, may be null
     * @return the decoded membership, or null when the segment recorded none
     */
    public static SegmentMembership decode(byte[] encoded) {
        if (encoded == null || encoded.length < 2) {
            return null;
        }

        if (encoded.length == SegmentFilter.SMALL_BYTES || encoded.length == SegmentFilter.CHUNK_BYTES) {
            // Segments written before this format carried a bare filter with no tag byte. An exact
            // list is always one byte plus a multiple of eight, and a tagged filter one byte more
            // than its size, so neither can ever be exactly a filter's length.
            return new SegmentMembership(null, SegmentFilter.fromBytes(encoded));
        }

        if (encoded[0] == TAG_EXACT) {
            int count = (encoded.length - 1) / 8;
            long[] values = new long[count];
            int offset = 1;
            for (int index = 0; index < count; index++) {
                long value = 0;
                for (int shift = 0; shift < 8; shift++) {
                    value = (value << 8) | (encoded[offset++] & 0xFFL);
                }
                values[index] = value;
            }
            return new SegmentMembership(values, null);
        }

        byte[] bytes = new byte[encoded.length - 1];
        System.arraycopy(encoded, 1, bytes, 0, bytes.length);
        return new SegmentMembership(null, SegmentFilter.fromBytes(bytes));
    }

    /**
     * @param value
     *            the value a lookup is asking for
     * @return true if the segment might contain it
     */
    public boolean mightContain(long value) {
        if (values != null) {
            return Arrays.binarySearch(values, value) >= 0;
        }
        return filter == null || filter.mightContain(value);
    }

    /**
     * @param wanted
     *            the values a lookup is asking for
     * @return true if the segment might contain any of them
     */
    public boolean mightContainAny(long[] wanted) {
        for (long value : wanted) {
            if (mightContain(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if the membership is an exact list rather than a filter
     */
    public boolean isExact() {
        return values != null;
    }
}

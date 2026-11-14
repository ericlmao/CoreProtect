package net.coreprotect.database;

import java.util.Arrays;

/**
 * How many rows a segment holds in each chunk, exactly, kept in memory for every segment.
 *
 * <p>
 * The chunk filter a segment has always carried becomes a Bloom filter once the segment covers
 * more than a few hundred chunks, and a busy segment always does. A lookup about one place then
 * opens every segment whose filter happens to say "maybe", which on a real server is a large share
 * of them. The exact counts say "no" whenever the answer is no, and they also say how many rows a
 * lookup covering whole chunks would find, so such a lookup can be counted and paged without
 * decoding the segment at all.
 * </p>
 *
 * <p>
 * Every segment's counts stay in memory, and a busy segment covers thousands of chunks, so they are
 * held as tightly as an exact answer allows. A segment's chunks lie in a few worlds and within a
 * few thousand chunks of each other, so each chunk is stored as its world's place in the segment's
 * short list of worlds and its distance from the segment's lowest chunk x and z, packed into one
 * int, and only into a long when the segment spans too far for that. Most chunks hold a few rows,
 * so a count takes one byte, and the few chunks holding more are listed apart. That is five bytes a
 * chunk, where a hashed key and an int count would take twelve. Chunks are kept in the order of
 * their world, then x, then z, so the chunks of a lookup's rectangle are found by searching for
 * each column of it rather than by testing chunks one at a time, however many there are.
 * </p>
 */
public final class SegmentChunkCounts {

    /** Bits for each coordinate's distance when a chunk is packed into an int. */
    private static final int NARROW_BITS = 14;
    /** Bits for the world's place when a chunk is packed into an int, leaving the sign bit clear. */
    private static final int NARROW_WORLD_BITS = 3;
    /** Bits for each coordinate's distance when a chunk is packed into a long; enough for any. */
    private static final int WIDE_BITS = 28;
    /** Bits for the world's place when a chunk is packed into a long, leaving the sign bit clear. */
    private static final int WIDE_WORLD_BITS = 7;

    /** The count a byte holds to say the real count is listed apart. */
    private static final int LISTED_APART = 255;

    private final int[] worldIds;
    private final int minimumX;
    private final int maximumX;
    private final int minimumZ;
    private final int maximumZ;
    private final int[] narrow;
    private final long[] wide;
    private final int bits;
    private final byte[] rows;
    private final int[] largePositions;
    private final int[] largeRows;
    private final int unlocatedRows;

    private SegmentChunkCounts(int[] worldIds, int minimumX, int maximumX, int minimumZ, int maximumZ, int[] narrow, long[] wide, int bits, byte[] rows, int[] largePositions,
            int[] largeRows, int unlocatedRows) {
        this.worldIds = worldIds;
        this.minimumX = minimumX;
        this.maximumX = maximumX;
        this.minimumZ = minimumZ;
        this.maximumZ = maximumZ;
        this.narrow = narrow;
        this.wide = wide;
        this.bits = bits;
        this.rows = rows;
        this.largePositions = largePositions;
        this.largeRows = largeRows;
        this.unlocatedRows = unlocatedRows;
    }

    /**
     * @param encoded
     *            the chunk counts stored beside a segment, may be null
     * @return the counts, or null when the segment has none, in which case its chunk filter is used
     */
    public static SegmentChunkCounts decode(byte[] encoded) {
        ColdSegmentCodec.ChunkList chunks = ColdSegmentCodec.decodeChunkCounts(encoded);
        return chunks == null ? null : of(chunks);
    }

    /**
     * @param chunks
     *            every chunk of a segment, in the order the codec lists them
     * @return the counts, or null if the segment has rows in more worlds than can be packed, which no
     *         real segment does, and which leaves it to its chunk filter
     */
    static SegmentChunkCounts of(ColdSegmentCodec.ChunkList chunks) {
        int size = chunks.size();
        int[] worlds = new int[size];
        int minimumX = Integer.MAX_VALUE;
        int maximumX = Integer.MIN_VALUE;
        int minimumZ = Integer.MAX_VALUE;
        int maximumZ = Integer.MIN_VALUE;
        for (int index = 0; index < size; index++) {
            worlds[index] = chunks.worldId(index);
            minimumX = Math.min(minimumX, chunks.chunkX(index));
            maximumX = Math.max(maximumX, chunks.chunkX(index));
            minimumZ = Math.min(minimumZ, chunks.chunkZ(index));
            maximumZ = Math.max(maximumZ, chunks.chunkZ(index));
        }
        int[] worldIds = Arrays.stream(worlds).distinct().sorted().toArray();
        if (worldIds.length > (1 << WIDE_WORLD_BITS)) {
            return null;
        }

        boolean fitsNarrow = size == 0 || (worldIds.length <= (1 << NARROW_WORLD_BITS) && (long) maximumX - minimumX < (1L << NARROW_BITS)
                && (long) maximumZ - minimumZ < (1L << NARROW_BITS));
        int bits = fitsNarrow ? NARROW_BITS : WIDE_BITS;
        int[] narrow = fitsNarrow ? new int[size] : null;
        long[] wide = fitsNarrow ? null : new long[size];
        byte[] rows = new byte[size];
        int large = 0;
        for (int index = 0; index < size; index++) {
            if (chunks.rows(index) >= LISTED_APART) {
                large++;
            }
        }
        int[] largePositions = new int[large];
        int[] largeRows = new int[large];
        large = 0;
        for (int index = 0; index < size; index++) {
            int world = Arrays.binarySearch(worldIds, chunks.worldId(index));
            long key = pack(world, (long) chunks.chunkX(index) - minimumX, (long) chunks.chunkZ(index) - minimumZ, bits);
            if (fitsNarrow) {
                narrow[index] = (int) key;
            }
            else {
                wide[index] = key;
            }
            int count = chunks.rows(index);
            if (count >= LISTED_APART) {
                rows[index] = (byte) LISTED_APART;
                largePositions[large] = index;
                largeRows[large] = count;
                large++;
            }
            else {
                rows[index] = (byte) count;
            }
        }
        // The codec lists chunks by world, then x, then z, and packing keeps that order, since the
        // world's place and both distances only grow where the coordinates do.
        return new SegmentChunkCounts(worldIds, minimumX, maximumX, minimumZ, maximumZ, narrow, wide, bits, rows, largePositions, largeRows, chunks.unlocatedRows());
    }

    private static long pack(long world, long distanceX, long distanceZ, int bits) {
        return (world << (bits * 2)) | (distanceX << bits) | distanceZ;
    }

    /**
     * Tells whether a lookup about a rectangle of chunks has to read this segment.
     *
     * @param worldId
     *            the world the lookup is about
     * @param minimumChunkX
     *            the lowest chunk x the lookup covers
     * @param maximumChunkX
     *            the highest chunk x the lookup covers
     * @param minimumChunkZ
     *            the lowest chunk z the lookup covers
     * @param maximumChunkZ
     *            the highest chunk z the lookup covers
     * @return true if the segment holds a row in any of those chunks, or holds a row without a
     *         place, which is never ruled out by where it is
     */
    public boolean mayHoldAny(int worldId, int minimumChunkX, int maximumChunkX, int minimumChunkZ, int maximumChunkZ) {
        return unlocatedRows > 0 || rowsIn(worldId, minimumChunkX, maximumChunkX, minimumChunkZ, maximumChunkZ, true) > 0;
    }

    /**
     * @return how many rows of the segment lie in a rectangle of chunks of one world; rows without a
     *         place lie in none
     */
    public long rowsIn(int worldId, int minimumChunkX, int maximumChunkX, int minimumChunkZ, int maximumChunkZ) {
        return rowsIn(worldId, minimumChunkX, maximumChunkX, minimumChunkZ, maximumChunkZ, false);
    }

    private long rowsIn(int worldId, int minimumChunkX, int maximumChunkX, int minimumChunkZ, int maximumChunkZ, boolean anyWillDo) {
        int size = rows.length;
        int world = Arrays.binarySearch(worldIds, worldId);
        if (size == 0 || world < 0) {
            return 0;
        }
        int lowX = Math.max(minimumChunkX, minimumX);
        int highX = Math.min(maximumChunkX, maximumX);
        int lowZ = Math.max(minimumChunkZ, minimumZ);
        int highZ = Math.min(maximumChunkZ, maximumZ);
        if (lowX > highX || lowZ > highZ) {
            return 0;
        }

        long total = 0;
        long columns = (long) highX - lowX + 1;
        if (columns * 32 > size) {
            // A rectangle with as many columns as the segment has chunks: walking every chunk once
            // costs less than searching for every column.
            for (int index = 0; index < size; index++) {
                long key = key(index);
                if ((key >>> (bits * 2)) != world) {
                    continue;
                }
                long x = minimumX + ((key >>> bits) & ((1L << bits) - 1));
                long z = minimumZ + (key & ((1L << bits) - 1));
                if (x >= lowX && x <= highX && z >= lowZ && z <= highZ) {
                    total = total + rows(index);
                    if (anyWillDo) {
                        return total;
                    }
                }
            }
            return total;
        }

        for (long x = lowX; x <= highX; x++) {
            long first = pack(world, x - minimumX, (long) lowZ - minimumZ, bits);
            long last = pack(world, x - minimumX, (long) highZ - minimumZ, bits);
            for (int index = lowerBound(first); index < size && key(index) <= last; index++) {
                total = total + rows(index);
                if (anyWillDo) {
                    return total;
                }
            }
        }
        return total;
    }

    private long key(int index) {
        return narrow != null ? narrow[index] : wide[index];
    }

    private int rows(int index) {
        int count = rows[index] & 0xFF;
        return count == LISTED_APART ? largeRows[Arrays.binarySearch(largePositions, index)] : count;
    }

    /** @return the position of the first chunk whose key is at least the one given */
    private int lowerBound(long key) {
        int low = 0;
        int high = rows.length;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (key(middle) < key) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        return low;
    }

    /**
     * @return how many distinct chunks the segment has rows in
     */
    public int distinctChunks() {
        return rows.length;
    }

    /**
     * @return how many rows of the segment lack a world or a coordinate
     */
    public int unlocatedRows() {
        return unlocatedRows;
    }

    /**
     * @return roughly how much memory these counts hold, on a 64-bit JVM with compressed references,
     *         for reporting what the index costs
     */
    public long heapBytes() {
        // The object itself: a header, six references and six ints, rounded up to eight bytes.
        long bytes = 64;
        bytes = bytes + array(worldIds.length * 4L);
        bytes = bytes + (narrow != null ? array(narrow.length * 4L) : array(wide.length * 8L));
        bytes = bytes + array(rows.length);
        bytes = bytes + array(largePositions.length * 4L) + array(largeRows.length * 4L);
        return bytes;
    }

    private static long array(long contents) {
        return (16 + contents + 7) & ~7L;
    }
}

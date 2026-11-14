package net.coreprotect.database;

/**
 * A small Bloom filter used to skip cold segments that cannot contain what a lookup is asking for.
 *
 * <p>
 * The filter answers "this segment definitely does not contain that value" with certainty and
 * "it might" otherwise, which is all segment skipping needs: a false positive costs one wasted
 * segment read, never a wrong result.
 * </p>
 */
public final class SegmentFilter {

    /** Filter size for chunk coordinates, which are the most numerous value per segment. */
    public static final int CHUNK_BYTES = 4096;

    /** Filter size for the far smaller sets of users and block types in one segment. */
    public static final int SMALL_BYTES = 512;

    private static final int HASHES = 4;

    private final byte[] bits;
    private final int mask;

    /**
     * @param bytes
     *            the filter size in bytes, which must be a power of two
     */
    public SegmentFilter(int bytes) {
        this(new byte[bytes]);
    }

    private SegmentFilter(byte[] bits) {
        if (bits.length == 0 || (bits.length & (bits.length - 1)) != 0) {
            throw new IllegalArgumentException("Filter size must be a power of two");
        }
        this.bits = bits;
        this.mask = (bits.length * 8) - 1;
    }

    /**
     * @param bytes
     *            filter contents previously returned by {@link #toBytes()}, may be null
     * @return the filter, or null when there is none
     */
    public static SegmentFilter fromBytes(byte[] bytes) {
        return bytes == null ? null : new SegmentFilter(bytes);
    }

    /**
     * Combines a world, chunk x and chunk z into the value a spatial filter stores.
     *
     * @param worldId
     *            the interned world id
     * @param chunkX
     *            the chunk x coordinate
     * @param chunkZ
     *            the chunk z coordinate
     * @return the filter value
     */
    public static long chunkKey(int worldId, int chunkX, int chunkZ) {
        long value = Integer.toUnsignedLong(worldId);
        value = mix64(value ^ (Integer.toUnsignedLong(chunkX) * 0x9E3779B97F4A7C15L));
        return mix64(value ^ (Integer.toUnsignedLong(chunkZ) * 0xC2B2AE3D27D4EB4FL));
    }

    /**
     * @param coordinate
     *            a block coordinate
     * @return the chunk the coordinate falls in
     */
    public static int chunkOf(int coordinate) {
        return Math.floorDiv(coordinate, 16);
    }

    /**
     * Places a chunk on a curve that keeps neighbouring chunks close together, by interleaving the
     * bits of its coordinates. Sorting by this puts the rows of one area of the world next to each
     * other, whichever direction the area extends in.
     *
     * @param chunkX
     *            a chunk x coordinate
     * @param chunkZ
     *            a chunk z coordinate
     * @return the chunk's position along the curve
     */
    public static long chunkCurve(int chunkX, int chunkZ) {
        // Shifted so negative coordinates sort below positive ones; chunk coordinates never need
        // more than 26 bits, and each of these has 32.
        return spread(Integer.toUnsignedLong(chunkX ^ Integer.MIN_VALUE)) | (spread(Integer.toUnsignedLong(chunkZ ^ Integer.MIN_VALUE)) << 1);
    }

    private static long spread(long value) {
        long spread = value & 0xFFFFFFFFL;
        spread = (spread | (spread << 16)) & 0x0000FFFF0000FFFFL;
        spread = (spread | (spread << 8)) & 0x00FF00FF00FF00FFL;
        spread = (spread | (spread << 4)) & 0x0F0F0F0F0F0F0F0FL;
        spread = (spread | (spread << 2)) & 0x3333333333333333L;
        spread = (spread | (spread << 1)) & 0x5555555555555555L;
        return spread;
    }

    public void add(long value) {
        long first = mix64(value);
        long second = mix64(value ^ 0xD6E8FEB86659FD93L) | 1L;
        for (int hash = 0; hash < HASHES; hash++) {
            int bit = (int) ((first + (second * hash)) & mask);
            bits[bit >>> 3] |= (byte) (1 << (bit & 7));
        }
    }

    public boolean mightContain(long value) {
        long first = mix64(value);
        long second = mix64(value ^ 0xD6E8FEB86659FD93L) | 1L;
        for (int hash = 0; hash < HASHES; hash++) {
            int bit = (int) ((first + (second * hash)) & mask);
            if ((bits[bit >>> 3] & (1 << (bit & 7))) == 0) {
                return false;
            }
        }
        return true;
    }

    public byte[] toBytes() {
        byte[] copy = new byte[bits.length];
        System.arraycopy(bits, 0, copy, 0, bits.length);
        return copy;
    }

    private static long mix64(long input) {
        long value = input;
        value ^= (value >>> 33);
        value *= 0xFF51AFD7ED558CCDL;
        value ^= (value >>> 33);
        value *= 0xC4CEB9FE1A85EC53L;
        value ^= (value >>> 33);
        return value;
    }
}

package net.coreprotect.database;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.SQLException;

import net.coreprotect.config.Config;

/**
 * Keeps the decompressed frames of recently read segments in memory.
 *
 * <p>
 * A segment is never rewritten once it is stored: its rows only ever leave with the whole segment,
 * and a rollback records its state beside it rather than in it. What a segment decompresses to is
 * therefore the same every time it is read, and a lookup that opens a segment a recent lookup
 * already opened can skip both reading it from the database and decompressing it. That is the
 * common case rather than the exception: the inspector clicked on neighbouring blocks, a lookup
 * paged through, a rollback previewed and then applied all read the same few segments again.
 * </p>
 *
 * <p>
 * The scalar frame of a segment is read first and the payload frame only once a lookup keeps a row
 * with a text or blob value in it, so a lookup that only needs to know which rows match, or keeps
 * none of a segment's rows, never reads the payload at all. Each frame is decompressed only when
 * first needed. Memory is bounded by the number of bytes held, and the least recently used segments
 * are let go first.
 * </p>
 *
 * <p>
 * A segment whose unit bodies are stored apart from its directory is held as its directory, plus
 * whichever unit bodies and payload lookups have read so far, each added as it is read. A second
 * lookup about the same place finds what it needs already here and reads nothing.
 * </p>
 */
final class SegmentFrameCache {

    /** Used when the configured size is unavailable. */
    private static final long DEFAULT_BYTES = 128L * 1024L * 1024L;

    private static final Map<Key, Entry> ENTRIES = new LinkedHashMap<>(64, 0.75f, true);

    private static long heldBytes;
    private static long hits;
    private static long misses;

    private SegmentFrameCache() {
        throw new IllegalStateException("Cache class");
    }

    /**
     * Identifies a segment by more than its id. Segment ids can be handed out again once the segment
     * holding one is dropped, but row ids never are, so a segment's first row id tells two segments
     * that shared an id apart.
     */
    private static final class Key {
        private final long id;
        private final long startRowId;

        private Key(long id, long startRowId) {
            this.id = id;
            this.startRowId = startRowId;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Key)) {
                return false;
            }
            Key key = (Key) other;
            return key.id == id && key.startRowId == startRowId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(id) * 31 + Long.hashCode(startRowId);
        }
    }

    /**
     * The stored frames of one segment, each decompressed once the first time it is needed. The
     * payload frame is added later, if a lookup ever keeps a row that needs it.
     */
    static final class Entry {
        private final byte[] storedScalars;
        private final int scalarSize;
        private final int payloadSize;
        private final int dictionaryId;
        private volatile byte[] storedPayload;
        private volatile boolean payloadHeld;
        private volatile byte[] scalars;
        private volatile byte[] payload;
        private volatile boolean held;
        /** The unit bodies read so far, for a directory whose units are stored apart. */
        private final Map<Integer, byte[]> units = new ConcurrentHashMap<>();
        private volatile long unitBytes;
        /** Whether each unit keeps its own share of the payload, as compacted segments do. */
        private final boolean payloadInUnits;
        /** The payload shares read so far, as stored, for a segment whose units keep them. */
        private final Map<Integer, byte[]> storedShares = new ConcurrentHashMap<>();
        /** The same shares decompressed, once a lookup has needed them. */
        private final Map<Integer, byte[]> shares = new ConcurrentHashMap<>();
        private volatile long shareBytes;
        private volatile int[] payloadStarts;

        /**
         * @param storedScalars
         *            the scalar frame as stored
         * @param scalarSize
         *            its size once decompressed
         * @param payloadSize
         *            the size the payload frame decompresses to, which is zero when the segment has
         *            no text or blob bytes and so nothing to read
         * @param dictionaryId
         *            the dictionary the payload frame was compressed with, or zero for none
         */
        Entry(byte[] storedScalars, int scalarSize, int payloadSize, int dictionaryId) {
            this(storedScalars, scalarSize, payloadSize, dictionaryId, false);
        }

        /**
         * @param payloadInUnits
         *            true when the segment row holds no payload and each unit row holds its own
         *            share of it instead
         */
        Entry(byte[] storedScalars, int scalarSize, int payloadSize, int dictionaryId, boolean payloadInUnits) {
            this.storedScalars = storedScalars;
            this.scalarSize = scalarSize;
            this.payloadSize = payloadSize;
            this.dictionaryId = dictionaryId;
            this.payloadInUnits = payloadInUnits && payloadSize > 0;
        }

        /**
         * @return true if each unit keeps its own share of the payload, so a lookup reads the shares
         *         of the units holding the values it keeps rather than one payload frame
         */
        boolean payloadInUnits() {
            return payloadInUnits;
        }

        /**
         * @return where each unit's share of the payload begins, worked out from the directory once
         * @throws SQLException
         *             if the directory cannot be read
         */
        int[] payloadStarts() throws SQLException {
            int[] starts = payloadStarts;
            if (starts == null) {
                try {
                    starts = ColdSegmentCodec.unitPayloadStarts(scalars());
                }
                catch (IllegalArgumentException malformed) {
                    throw new SQLException("A compressed segment has an unreadable directory", malformed);
                }
                payloadStarts = starts;
            }
            return starts;
        }

        /**
         * @param unit
         *            a unit number
         * @return the unit's share of the payload as stored, if it has been read, or null
         */
        byte[] storedShare(int unit) {
            return storedShares.get(unit);
        }

        /**
         * Holds a unit's share of the payload that was just read, and counts it against the cache
         * while the entry is held.
         *
         * @param unit
         *            the unit number
         * @param stored
         *            the share as stored
         */
        void holdShare(int unit, byte[] stored) {
            synchronized (ENTRIES) {
                if (storedShares.putIfAbsent(unit, stored) != null) {
                    return;
                }
                shareBytes = shareBytes + stored.length;
                if (held) {
                    heldBytes = heldBytes + stored.length;
                    trim(capacityBytes());
                }
            }
        }

        /**
         * Gives the shares of the payload a lookup needs, decompressing each the first time. The
         * shares have to have been read, and the dictionary loaded, on a thread holding a
         * connection first.
         *
         * @param wanted
         *            the units whose shares are needed
         * @return the payload, holding those shares
         * @throws SQLException
         *             if a share was not read or cannot be decompressed
         */
        ColdSegmentCodec.PayloadShares shares(int[] wanted) throws SQLException {
            int[] starts = payloadStarts();
            byte[][] ready = new byte[starts.length][];
            for (int unit : wanted) {
                byte[] share = shares.get(unit);
                if (share == null) {
                    byte[] stored = storedShares.get(unit);
                    if (stored == null) {
                        throw new SQLException("The payload of unit " + unit + " was used before it was read");
                    }
                    int end = unit + 1 < starts.length ? starts[unit + 1] : payloadSize;
                    ColdSegmentReadCounter.recordPayloadRead();
                    share = SegmentDictionary.decompress(stored, end - starts[unit], dictionaryId, null);
                    if (share.length != end - starts[unit]) {
                        throw new SQLException("The payload of unit " + unit + " decompressed to " + share.length + " bytes instead of " + (end - starts[unit]));
                    }
                    if (shares.putIfAbsent(unit, share) == null) {
                        synchronized (ENTRIES) {
                            shareBytes = shareBytes + share.length;
                        }
                        grow(this, share.length);
                    }
                }
                ready[unit] = share;
            }
            return new ColdSegmentCodec.PayloadShares(starts, ready);
        }

        /**
         * @return true if some of the given shares still have to be decompressed with a dictionary,
         *         which has to be loaded first on a thread that holds a connection
         */
        boolean sharesNeedDictionary(int[] wanted) {
            if (dictionaryId <= 0) {
                return false;
            }
            for (int unit : wanted) {
                if (!shares.containsKey(unit)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @return the scalar frame, ready to decode
         * @throws SQLException
         *             if it cannot be decompressed
         */
        synchronized byte[] scalars() throws SQLException {
            byte[] ready = scalars;
            if (ready == null) {
                // Frames written since groups existed are stored as they are decoded. Older ones were
                // compressed as a whole and unpack to about a megabyte each; those are decompressed on
                // each use rather than held, or a page that reads seventy of them would push
                // everything else out of the cache and find nothing in it for the next page.
                if (ColdSegmentCodec.isSelfContained(storedScalars)) {
                    ready = storedScalars;
                    scalars = ready;
                }
                else {
                    ready = SegmentDictionary.decompress(storedScalars, scalarSize, 0, null);
                }
            }
            return ready;
        }

        /**
         * @return the payload frame, decompressed; empty when the segment has none
         * @throws SQLException
         *             if it cannot be decompressed, or has not been read yet
         */
        synchronized byte[] payload() throws SQLException {
            byte[] ready = payload;
            if (ready == null) {
                if (!hasPayload()) {
                    throw new SQLException("The payload frame of this segment was used before it was read");
                }
                if (storedPayload == null) {
                    ready = new byte[0];
                }
                else {
                    ColdSegmentReadCounter.recordPayloadRead();
                    ready = SegmentDictionary.decompress(storedPayload, payloadSize, dictionaryId, null);
                }
                payload = ready;
                grow(this, ready.length);
            }
            return ready;
        }

        /**
         * @return true if the payload still has to be decompressed with a dictionary, which has to
         *         be loaded first on a thread that holds a connection
         */
        boolean needsDictionary() {
            return payload == null && storedPayload != null && dictionaryId > 0;
        }

        int dictionaryId() {
            return dictionaryId;
        }

        int scalarSize() {
            return scalarSize;
        }

        int payloadSize() {
            return payloadSize;
        }

        /**
         * @return true if the payload frame is in memory, or there is none to read
         */
        boolean hasPayload() {
            return payloadHeld || payloadSize == 0;
        }

        /**
         * Holds the stored payload frame once it has been read, and counts it against the cache
         * while the entry is held. A payload another lookup already added is kept rather than
         * replaced, since both are the same bytes.
         *
         * @param stored
         *            the payload frame as stored, or null when the segment stored none
         */
        void holdPayload(byte[] stored) {
            synchronized (ENTRIES) {
                if (payloadHeld) {
                    return;
                }
                storedPayload = stored;
                payloadHeld = true;
                if (held && stored != null) {
                    heldBytes = heldBytes + stored.length;
                    trim(capacityBytes());
                }
            }
        }

        /**
         * @return true if the stored scalar frame is a directory whose unit bodies are stored apart
         */
        boolean isSplit() {
            return ColdSegmentCodec.isSplit(storedScalars);
        }

        /**
         * @param unit
         *            a unit number
         * @return the unit's stored body if it has been read, or null
         */
        byte[] unit(int unit) {
            return units.get(unit);
        }

        /**
         * Holds a unit body that was just read, and counts it against the cache while the entry is
         * held. A body another lookup already added is kept rather than replaced, since both are
         * the same bytes.
         *
         * @param unit
         *            the unit number
         * @param body
         *            its stored body
         */
        void holdUnit(int unit, byte[] body) {
            synchronized (ENTRIES) {
                if (units.putIfAbsent(unit, body) != null) {
                    return;
                }
                unitBytes = unitBytes + body.length;
                if (held) {
                    heldBytes = heldBytes + body.length;
                    trim(capacityBytes());
                }
            }
        }

        private long size() {
            byte[] readyScalars = scalars;
            byte[] readyPayload = payload;
            return storedScalars.length + (storedPayload == null ? 0 : storedPayload.length) + unitBytes + shareBytes
                    + (readyScalars == null || readyScalars == storedScalars ? 0 : readyScalars.length) + (readyPayload == null ? 0 : readyPayload.length);
        }
    }

    /**
     * @param segment
     *            a segment
     * @return its cached frames, or null when they are not held
     */
    static Entry get(SQLiteColdIndex.ColdSegment segment) {
        synchronized (ENTRIES) {
            Entry entry = ENTRIES.get(new Key(segment.id, segment.startRowId));
            if (entry == null) {
                misses++;
            }
            else {
                hits++;
            }
            return entry;
        }
    }

    /**
     * Holds the frames of a segment that were just read.
     *
     * @param segment
     *            the segment they belong to
     * @param entry
     *            the frames
     * @return the entry now held for that segment, which is an earlier one if another read got there
     *         first
     */
    static Entry put(SQLiteColdIndex.ColdSegment segment, Entry entry) {
        long capacity = capacityBytes();
        if (capacity <= 0 || entry.storedScalars.length > capacity / 4) {
            return entry; // too big to be worth holding, or caching is switched off
        }

        synchronized (ENTRIES) {
            Key key = new Key(segment.id, segment.startRowId);
            Entry existing = ENTRIES.get(key);
            if (existing != null) {
                return existing;
            }
            ENTRIES.put(key, entry);
            entry.held = true;
            heldBytes = heldBytes + entry.size();
            trim(capacity);
            return entry;
        }
    }

    /**
     * Lets go of one segment, for when it has been rewritten.
     *
     * @param segment
     *            the segment
     */
    static void forget(SQLiteColdIndex.ColdSegment segment) {
        synchronized (ENTRIES) {
            Entry removed = ENTRIES.remove(new Key(segment.id, segment.startRowId));
            if (removed != null) {
                removed.held = false;
                heldBytes = heldBytes - removed.size();
            }
        }
    }

    private static void grow(Entry entry, long bytes) {
        synchronized (ENTRIES) {
            if (entry.held) {
                heldBytes = heldBytes + bytes;
                trim(capacityBytes());
            }
        }
    }

    /**
     * Lets go of every segment that is no longer stored.
     *
     * @param stored
     *            every segment the database holds now
     */
    static void retain(Collection<SQLiteColdIndex.ColdSegment> stored) {
        Set<Key> keep = new HashSet<>();
        for (SQLiteColdIndex.ColdSegment segment : stored) {
            keep.add(new Key(segment.id, segment.startRowId));
        }
        synchronized (ENTRIES) {
            Iterator<Map.Entry<Key, Entry>> iterator = ENTRIES.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Key, Entry> held = iterator.next();
                if (!keep.contains(held.getKey())) {
                    held.getValue().held = false;
                    heldBytes = heldBytes - held.getValue().size();
                    iterator.remove();
                }
            }
        }
    }

    /** Lets go of everything, for when the database itself is let go of. */
    static void clear() {
        synchronized (ENTRIES) {
            for (Entry entry : ENTRIES.values()) {
                entry.held = false;
            }
            ENTRIES.clear();
            heldBytes = 0;
            hits = 0;
            misses = 0;
        }
    }

    /**
     * @return how many decompressed bytes are held
     */
    static long heldBytes() {
        synchronized (ENTRIES) {
            return heldBytes;
        }
    }

    /**
     * @return how many lookups of a segment found it held
     */
    static long hits() {
        synchronized (ENTRIES) {
            return hits;
        }
    }

    /**
     * @return how many lookups of a segment had to read it
     */
    static long misses() {
        synchronized (ENTRIES) {
            return misses;
        }
    }

    private static void trim(long capacity) {
        Iterator<Map.Entry<Key, Entry>> iterator = ENTRIES.entrySet().iterator();
        while (heldBytes > capacity && iterator.hasNext()) {
            Map.Entry<Key, Entry> eldest = iterator.next();
            eldest.getValue().held = false;
            heldBytes = heldBytes - eldest.getValue().size();
            iterator.remove();
        }
    }

    private static long capacityBytes() {
        Config config = Config.getGlobal();
        if (config == null) {
            return DEFAULT_BYTES;
        }
        return config.LOOKUP_CACHE_MB < 0 ? DEFAULT_BYTES : config.LOOKUP_CACHE_MB * 1024L * 1024L;
    }
}

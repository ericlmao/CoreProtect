package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.coreprotect.config.ConfigHandler;

/**
 * Merges the segments of every day that is over into a few large ones, ordered by place across the
 * whole day.
 *
 * <p>
 * Packing writes a segment as soon as a table has logged a segment's worth of rows, so a busy table
 * writes one every few minutes and hundreds a day, each ordered by place within itself only. The
 * activity at one chest over one day is spread across every segment written while anyone used it,
 * and a lookup about that chest has to open each of them: over half a year that is thousands of
 * segments. Once a day is over nothing more is written to it, so its segments are merged into a few
 * ordered by place across the whole day. The rows of one place over one day then lie together in
 * one segment, usually in one group of it, and a lookup over any length of history opens about one
 * segment for each day the place saw activity.
 * </p>
 *
 * <p>
 * Only a run of segments that follow one another in row ids, with no other segment of the table in
 * between, is merged, and only segments of one day. The merged segments each claim the run's whole
 * range of row ids, since rows ordered by place come from anywhere in it; they never overlap any
 * other segment, so everything that orders segments by row id still finds the older rows below the
 * newer ones, as long as it takes the segments sharing a range together. Rows keep their row ids, so
 * rollback state recorded beside packed rows still applies.
 * </p>
 *
 * <p>
 * Every input is already ordered, so they are merged rather than sorted: each is read a group of
 * rows at a time and the next row out is always the lowest of the rows at the front of the inputs.
 * At most {@link #MAXIMUM_INPUTS} are merged at once, so the decoded rows held never exceed
 * {@link #MAXIMUM_HELD_ROWS}; a day with more segments than that is merged in passes, first
 * stretches of freshly packed segments into longer ordered ones, then those into the day. Besides
 * those rows, each input holds the directory of the segment it is reading, one unit's body, and
 * that unit's text and blob bytes: its own share for a compacted segment, or the whole payload frame
 * of a segment written by packing, which is at most one packed segment's worth, typically well
 * under two megabytes. The output holds one unit's encoded columns and the compressed units and
 * shares of the segment being written, a few megabytes for {@link #TARGET_ROWS} rows. Nothing ever
 * holds a whole segment's rows.
 * </p>
 *
 * <p>
 * The merged segments are written aside first, each in a short transaction of its own, marked so
 * that nothing reads them. They take the place of their inputs in one transaction, committed while
 * lookups are held off, so a lookup sees either the inputs or the outputs and never both or neither.
 * A compaction interrupted before that leaves the inputs as they were, and what it wrote aside is
 * removed by the next one.
 * </p>
 *
 * <p>
 * The {@code compacted} column of a segment records how far it has come: {@link #PACKED} as packing
 * wrote it, {@link #MERGED} when a pass has merged it with others but its day has more to merge,
 * {@link #COMPACTED} once its whole run is merged, and {@link #WRITING} while compaction is still
 * writing it.
 * </p>
 */
public final class SegmentCompactor {

    /** A segment being written by compaction, which nothing but that compaction may see. */
    static final int WRITING = -1;

    /** A segment as packing wrote it. */
    static final int PACKED = 0;

    /** A segment merged from others by a pass that did not yet reach the rest of its day. */
    static final int MERGED = 1;

    /** A segment whose run has been merged completely, which is never merged again. */
    static final int COMPACTED = 2;

    /**
     * The most inputs merged at once. Each holds one group of decoded rows, so this is what bounds
     * the memory a merge takes; a day with more segments is merged in passes.
     */
    static final int MAXIMUM_INPUTS = 64;

    /**
     * The rows compaction aims to put in each segment it writes. A run is divided into as few
     * segments as this allows, all of about the same size.
     */
    static final int TARGET_ROWS = 262_144;

    /** The most decoded rows a merge ever holds: a group for each input and the group being written. */
    static final long MAXIMUM_HELD_ROWS = (long) (MAXIMUM_INPUTS + 1) * ColdSegmentCodec.GROUP_ROWS;

    /** How many times the swap is tried while lookups keep running, before the merge is let go. */
    private static final int SWAP_ATTEMPTS = 3;

    private static volatile int targetRows = TARGET_ROWS;

    /** Decoded rows held by the merge in progress. */
    private static final AtomicLong HELD_ROWS = new AtomicLong();

    /** The most decoded rows any merge has held at once. */
    private static final AtomicLong PEAK_HELD_ROWS = new AtomicLong();

    private SegmentCompactor() {
        throw new IllegalStateException("Task class");
    }

    /**
     * Changes the rows compaction aims to put in a segment, for tests and for benchmarks comparing
     * sizes.
     *
     * @param rows
     *            rows per segment, or 0 to go back to {@link #TARGET_ROWS}
     */
    static void setTargetRowsForTesting(int rows) {
        targetRows = rows <= 0 ? TARGET_ROWS : rows;
    }

    /**
     * @return the most decoded rows any merge has held at once since the last reset
     */
    static long peakHeldRows() {
        return PEAK_HELD_ROWS.get();
    }

    /** Starts measuring {@link #peakHeldRows()} afresh. */
    static void resetPeakHeldRows() {
        PEAK_HELD_ROWS.set(HELD_ROWS.get());
    }

    /**
     * Takes one step of compaction if there is one to take and nothing else is packing or rewriting
     * segments: merges one run, or one pass of a long run. Used by the packing that runs in the
     * background, between which logging and packing get their turn.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment is written; throwing stops the compaction
     * @return how much was done: rows merged or segments found to need no merging, 0 when there was
     *         nothing to do
     * @throws Exception
     *             if compaction fails or is stopped
     */
    public static long compactNext(Connection connection, ColdRollupTask.Callback callback) throws Exception {
        if (!ColdRollupTask.SEALING.tryLock(0, TimeUnit.MILLISECONDS)) {
            return 0;
        }
        try {
            return step(connection, callback, ColdRollupTask.openDay());
        }
        finally {
            ColdRollupTask.SEALING.unlock();
        }
    }

    /**
     * Compacts every day that is over, for the manual compact command and the nightly maintenance.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment is written; throwing stops the compaction
     * @return how much was done: rows merged and segments found to need no merging
     * @throws Exception
     *             if compaction fails or is stopped
     */
    public static long compactAll(Connection connection, ColdRollupTask.Callback callback) throws Exception {
        long done = 0;
        while (true) {
            ColdRollupTask.SEALING.lockInterruptibly();
            long step;
            try {
                step = step(connection, callback, ColdRollupTask.openDay());
            }
            finally {
                ColdRollupTask.SEALING.unlock();
            }
            if (step <= 0) {
                return done;
            }
            done = done + step;
        }
    }

    /**
     * Tells cheaply whether there is anything to compact, from an index alone.
     *
     * @param connection
     *            an open connection
     * @param openDay
     *            the first day that may still receive rows
     * @return true if a segment of a day that is over has not been compacted yet, or something an
     *         interrupted compaction wrote aside is waiting to be removed
     * @throws SQLException
     *             if the database cannot be read
     */
    private static boolean hasWork(Connection connection, long openDay) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM " + ConfigHandler.prefix + "segment WHERE compacted < 0 OR (compacted IN (" + PACKED + ","
                + MERGED + ") AND codec_version = ? AND day < ?) LIMIT 1")) {
            statement.setInt(1, ColdSegmentCodec.CODEC_VERSION);
            statement.setLong(2, openDay);
            try (ResultSet results = statement.executeQuery()) {
                return results.next();
            }
        }
    }

    private static long step(Connection connection, ColdRollupTask.Callback callback, long openDay) throws Exception {
        if (!hasWork(connection, openDay)) {
            return 0;
        }
        SQLiteColdIndex.ensureLoaded(connection);
        removeAbandoned(connection);

        long marked = 0;
        for (String table : SQLiteColdIndex.getSegmentedTables()) {
            Integer tableId = SQLiteColdIndex.tableId(table);
            SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, table);
            if (tableId == null || layout == null) {
                continue;
            }

            List<List<Stream>> runs = runs(connection, tableId, openDay);
            // A run that is a single segment, or a single merged stretch, is already ordered as a
            // whole. It is only recorded as done.
            List<Long> single = new ArrayList<>();
            for (List<Stream> run : runs) {
                if (run.size() == 1) {
                    for (Stored segment : run.get(0).segments) {
                        single.add(segment.id);
                    }
                }
            }
            marked = marked + markCompacted(connection, single);

            for (List<Stream> run : runs) {
                if (run.size() > 1 && !setAside(run)) {
                    try {
                        long merged = merge(connection, table, tableId, layout, run, callback);
                        return marked + merged;
                    }
                    catch (ColdRollupTask.SealDeferred deferred) {
                        // Lookups kept reading. Nothing was changed, and the run is merged later.
                        SQLiteColdIndex.debug("compact " + table + ": deferred while lookups finish");
                        return marked;
                    }
                    catch (Exception failure) {
                        if (!SegmentQuarantine.concernsSegment(failure)) {
                            throw failure;
                        }
                        // Which segment of the run is at fault is not known, so the whole run is set
                        // aside, left exactly as it was, and the other runs are merged.
                        SegmentQuarantine.add(ids(run), "merging a day of " + table, failure);
                        return marked;
                    }
                }
            }
        }
        return marked;
    }

    /**
     * Thrown when the segments a merge started from changed under it, a purge having dropped one of
     * them. Nothing is wrong with any segment, so the run is not set aside; it is simply merged again
     * from what is there on a later pass.
     */
    static final class RunChanged extends SQLException {
        private static final long serialVersionUID = 1L;

        RunChanged(String message) {
            super(message);
        }
    }

    /**
     * @return true if any segment of a run has been set aside after an earlier merge of it failed
     */
    private static boolean setAside(List<Stream> run) {
        for (Stream stream : run) {
            for (Stored segment : stream.segments) {
                if (SegmentQuarantine.contains(segment.id)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<Long> ids(List<Stream> run) {
        List<Long> ids = new ArrayList<>();
        for (Stream stream : run) {
            for (Stored segment : stream.segments) {
                ids.add(segment.id);
            }
        }
        return ids;
    }

    /** A segment as the compactor finds it in the database. */
    private static final class Stored {
        private final long id;
        private final long startRowId;
        private final long endRowId;
        private final int rowCount;
        private final long day;
        private final int compacted;
        private final int codecVersion;

        private Stored(ResultSet results) throws SQLException {
            this.id = results.getLong("id");
            this.startRowId = results.getLong("start_rowid");
            this.endRowId = results.getLong("end_rowid");
            this.rowCount = results.getInt("row_count");
            this.day = results.getLong("day");
            this.compacted = results.getInt("compacted");
            this.codecVersion = results.getInt("codec_version");
        }
    }

    /**
     * Segments sharing one range of row ids, whose rows follow on from one segment to the next in
     * lookup order: a segment written by packing on its own, or the segments one merge wrote, in the
     * order it wrote them.
     */
    private static final class Stream {
        private final List<Stored> segments = new ArrayList<>();

        private Stored first() {
            return segments.get(0);
        }

        private long rows() {
            long rows = 0;
            for (Stored segment : segments) {
                rows = rows + segment.rowCount;
            }
            return rows;
        }

        /**
         * @return true if every segment is of a day that is over, in the current format, and not yet
         *         compacted completely
         */
        private boolean mayMerge(long openDay) {
            for (Stored segment : segments) {
                if ((segment.compacted != PACKED && segment.compacted != MERGED) || segment.codecVersion != ColdSegmentCodec.CODEC_VERSION || segment.day >= openDay
                        || segment.day != first().day) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Finds the runs of one table that still need compacting: stretches of streams that follow one
     * another in row ids, with nothing of the table in between, all of one day that is over.
     */
    private static List<List<Stream>> runs(Connection connection, int tableId, long openDay) throws SQLException {
        List<Stream> streams = new ArrayList<>();
        String query = "SELECT id,start_rowid,end_rowid,row_count,day,compacted,codec_version FROM " + ConfigHandler.prefix + "segment WHERE table_id = ? AND compacted >= 0 ORDER BY start_rowid,id";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, tableId);
            try (ResultSet results = statement.executeQuery()) {
                Stream current = null;
                while (results.next()) {
                    Stored segment = new Stored(results);
                    if (current == null || current.first().startRowId != segment.startRowId || current.first().endRowId != segment.endRowId) {
                        current = new Stream();
                        streams.add(current);
                    }
                    current.segments.add(segment);
                }
            }
        }

        List<List<Stream>> runs = new ArrayList<>();
        List<Stream> run = null;
        for (Stream stream : streams) {
            if (!stream.mayMerge(openDay)) {
                run = null;
                continue;
            }
            if (run == null || run.get(0).first().day != stream.first().day) {
                run = new ArrayList<>();
                runs.add(run);
            }
            run.add(stream);
        }
        return runs;
    }

    /**
     * Records segments as compacted without rewriting them. Nothing a lookup reads changes, so
     * lookups need not be held off.
     */
    private static long markCompacted(Connection connection, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return 0;
        }
        int[] updated = new int[1];
        Database.inTransaction(connection, () -> {
            try (Statement statement = connection.createStatement()) {
                updated[0] = statement.executeUpdate("UPDATE " + ConfigHandler.prefix + "segment SET compacted = " + COMPACTED + " WHERE compacted IN (" + PACKED + "," + MERGED
                        + ") AND id IN (" + join(ids) + ")");
            }
        });
        return updated[0];
    }

    /**
     * Chooses which streams of a run one merge takes. A run short enough is merged whole. A longer
     * one is merged in passes: stretches of segments straight from packing first, each into one
     * ordered stream, so that every row is rewritten about twice however long the day was, and then
     * the streams those passes wrote.
     */
    private static List<Stream> choose(List<Stream> run) {
        if (run.size() <= MAXIMUM_INPUTS) {
            return run;
        }
        for (int start = 0; start + 1 < run.size(); start++) {
            if (run.get(start).first().compacted == PACKED && run.get(start + 1).first().compacted == PACKED) {
                int end = start;
                while (end < run.size() && end - start < MAXIMUM_INPUTS && run.get(end).first().compacted == PACKED) {
                    end++;
                }
                return run.subList(start, end);
            }
        }
        return run.subList(0, MAXIMUM_INPUTS);
    }

    /**
     * Merges one run, or one pass of it, and swaps the result in.
     *
     * @return how many rows were merged
     */
    private static long merge(Connection connection, String table, int tableId, SQLiteColdIndex.TableLayout layout, List<Stream> run, ColdRollupTask.Callback callback) throws Exception {
        List<Stream> chosen = choose(run);
        int level = chosen.size() == run.size() ? COMPACTED : MERGED;
        long total = 0;
        List<Long> inputs = new ArrayList<>();
        for (Stream stream : chosen) {
            total = total + stream.rows();
            for (Stored segment : stream.segments) {
                inputs.add(segment.id);
            }
        }
        long startRowId = chosen.get(0).first().startRowId;
        long endRowId = chosen.get(chosen.size() - 1).first().endRowId;

        // As few segments as the target allows, all of about the same size, rather than full ones
        // and a small remainder.
        int target = targetRows;
        long outputs = Math.max(1, (total + target - 1) / target);
        long base = total / outputs;
        long extra = total % outputs;

        int dictionaryId = SegmentDictionary.currentDictionary(connection, tableId);
        SegmentDictionary.warm(dictionaryId, connection);
        ColdSegmentCodec.PayloadPacker packer = raw -> SegmentDictionary.compressWith(raw, dictionaryId);
        ColdRollupTask.LookupOrder order = ColdRollupTask.LookupOrder.of(table, layout);
        boolean hasPayload = ColdSegmentCodec.hasPayload(layout.types);

        long started = System.nanoTime();
        List<Long> staged = new ArrayList<>();
        List<StreamCursor> cursors = new ArrayList<>();
        List<Object[]> groupRows = new ArrayList<>(ColdSegmentCodec.GROUP_ROWS);
        boolean swapped = false;
        try {
            PriorityQueue<StreamCursor> queue = new PriorityQueue<>((first, second) -> ColdRollupTask.LookupOrder.compare(first.key, second.key));
            for (Stream stream : chosen) {
                StreamCursor cursor = new StreamCursor(connection, order, stream.segments);
                cursors.add(cursor);
                if (cursor.next()) {
                    queue.add(cursor);
                }
            }

            long[] groupIds = new long[ColdSegmentCodec.GROUP_ROWS];
            long[] previous = new long[order.keyLength()];
            boolean first = true;
            long outOfOrder = 0;
            long merged = 0;
            for (long output = 0; output < outputs; output++) {
                long size = base + (output < extra ? 1 : 0);
                ColdSegmentCodec.Encoder encoder = new ColdSegmentCodec.Encoder(layout.types, layout.chunkColumns(), hasPayload ? packer : null);
                ColdRollupTask.Summary summary = new ColdRollupTask.Summary();
                long written = 0;
                while (written < size) {
                    StreamCursor cursor = queue.poll();
                    if (cursor == null) {
                        throw new SQLException("Compacting " + table + " ran out of rows after " + (merged + written) + " of " + total);
                    }
                    // Inputs are ordered, so the rows come out ordered. One that does not has come
                    // from a segment ordered some other way; it is merged all the same, since the
                    // order only makes lookups quicker and never changes what they find.
                    if (!first && ColdRollupTask.LookupOrder.compare(cursor.key, previous) < 0) {
                        outOfOrder++;
                    }
                    System.arraycopy(cursor.key, 0, previous, 0, previous.length);
                    first = false;

                    long rowId = cursor.rowId();
                    Object[] values = cursor.take();
                    groupIds[groupRows.size()] = rowId;
                    groupRows.add(values);
                    summary.add(rowId, values, layout);
                    written++;
                    if (cursor.next()) {
                        queue.add(cursor);
                    }
                    if (groupRows.size() == ColdSegmentCodec.GROUP_ROWS || written == size) {
                        encoder.addGroup(groupIds, groupRows, groupRows.size());
                        release(groupRows.size());
                        groupRows.clear();
                    }
                }

                ColdSegmentCodec.Encoded encoded = encoder.finish();
                staged.add(stage(connection, tableId, summary, startRowId, endRowId, encoded, dictionaryId));
                merged = merged + written;
                CompactProgress.set("compacting " + table, merged, total);
                callback.beforeSegment();
            }

            if (!queue.isEmpty()) {
                throw new SQLException("Compacting " + table + " left rows over after " + total);
            }
            for (StreamCursor cursor : cursors) {
                cursor.finish();
            }

            swap(connection, table, inputs, staged, level, callback);
            swapped = true;
            SQLiteColdIndex.debug("compact " + table + ": " + inputs.size() + " segments of " + chosen.size() + " streams, " + total + " rows, into " + staged.size() + " segments"
                    + (level == MERGED ? " (part of a longer day)" : "") + ", " + (System.nanoTime() - started) / 1_000_000L + " ms"
                    + (outOfOrder > 0 ? ", " + outOfOrder + " rows out of order" : ""));
            try (Statement statement = connection.createStatement()) {
                // Hand the pages the merged segments held back to the file system, a little at a time.
                statement.executeUpdate("PRAGMA incremental_vacuum(4000)");
            }
            return total;
        }
        finally {
            for (StreamCursor cursor : cursors) {
                cursor.close();
            }
            release(groupRows.size());
            if (!swapped && !staged.isEmpty()) {
                discard(connection, staged);
            }
        }
    }

    /**
     * Writes one merged segment aside, where nothing reads it until the swap.
     *
     * @return its id
     */
    private static long stage(Connection connection, int tableId, ColdRollupTask.Summary summary, long startRowId, long endRowId, ColdSegmentCodec.Encoded encoded, int dictionaryId) throws SQLException {
        long[] id = new long[1];
        boolean sharesPayload = encoded.getPayloadSize() > 0;
        Database.inTransaction(connection, () -> {
            id[0] = ColdRollupTask.insertSegmentRow(connection, tableId, summary, startRowId, endRowId, encoded.getDirectory(), null, encoded.getPayloadSize(), encoded.getChunkCounts(),
                    dictionaryId, WRITING);
            ColdRollupTask.writeUnits(connection, id[0], encoded.getUnits(), sharesPayload ? encoded.getUnitPayloads() : null);
        });
        return id[0];
    }

    /**
     * Puts the merged segments in place of their inputs.
     *
     * <p>
     * One transaction publishes the one and removes the other, and it commits only while no lookup
     * is running, with the metadata in memory brought up to date before any can start again. A
     * lookup therefore sees either the inputs or the outputs, never both and never neither. If an
     * input has gone meanwhile, a purge having dropped its day, nothing is swapped.
     * </p>
     */
    private static void swap(Connection connection, String table, List<Long> inputs, List<Long> outputs, int level, ColdRollupTask.Callback callback) throws Exception {
        for (int attempt = 1;; attempt++) {
            try {
                swapOnce(connection, table, inputs, outputs, level);
                return;
            }
            catch (ColdRollupTask.SealDeferred deferred) {
                if (attempt >= SWAP_ATTEMPTS) {
                    throw deferred;
                }
                callback.beforeSegment();
            }
        }
    }

    private static void swapOnce(Connection connection, String table, List<Long> inputs, List<Long> outputs, int level) throws SQLException {
        String prefix = ConfigHandler.prefix;
        boolean[] locked = new boolean[1];
        boolean committed = false;
        ColdRollupTask.awaitLookups();
        try {
            Database.inTransaction(connection, () -> {
                try (Statement statement = connection.createStatement()) {
                    int published = statement.executeUpdate("UPDATE " + prefix + "segment SET compacted = " + level + " WHERE compacted = " + WRITING + " AND id IN (" + join(outputs) + ")");
                    if (published != outputs.size()) {
                        throw new RunChanged("Only " + published + " of " + outputs.size() + " compacted segments of " + table + " were still waiting to be swapped in");
                    }
                    int removed = statement.executeUpdate("DELETE FROM " + prefix + "segment WHERE compacted IN (" + PACKED + "," + MERGED + ") AND id IN (" + join(inputs) + ")");
                    if (removed != inputs.size()) {
                        throw new RunChanged("Only " + removed + " of the " + inputs.size() + " segments of " + table + " being compacted were still there");
                    }
                    statement.executeUpdate("DELETE FROM " + prefix + "segment_unit WHERE segment_id IN (" + join(inputs) + ")");
                }
                // The update above has taken the database's write lock, so no writer can be inside a
                // transaction now, and waiting for lookups here cannot wait on one that waits for us.
                ColdRollupTask.holdOffLookups(locked);
            });
            committed = true;
            SQLiteColdIndex.replaceSegments(connection, table, inputs, outputs);
        }
        catch (SQLException failure) {
            if (committed) {
                // The segments were swapped but the index does not know it. Forgetting it makes the
                // next lookup read it afresh rather than read segments that are gone.
                SQLiteColdIndex.invalidate();
            }
            throw failure;
        }
        finally {
            if (locked[0]) {
                SQLiteColdIndex.unlockAfterSeal();
            }
        }
    }

    /**
     * Removes what an interrupted compaction wrote aside. Called only while holding the lock every
     * compaction holds, so nothing is still writing it.
     */
    private static void removeAbandoned(Connection connection) throws SQLException {
        List<Long> abandoned = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT id FROM " + ConfigHandler.prefix + "segment WHERE compacted = " + WRITING)) {
            while (results.next()) {
                abandoned.add(results.getLong(1));
            }
        }
        if (!abandoned.isEmpty()) {
            SQLiteColdIndex.debug("compact: removing " + abandoned.size() + " segments an interrupted compaction left unfinished");
            discardNow(connection, abandoned);
        }
    }

    /** Removes segments written aside, after a merge that did not finish. */
    private static void discard(Connection connection, List<Long> staged) {
        try {
            discardNow(connection, staged);
        }
        catch (SQLException failure) {
            // Whatever stopped the merge is the failure worth reporting. The next compaction
            // removes what is left.
            SQLiteColdIndex.debug("compact: could not remove unfinished segments yet (" + failure.getMessage() + ")");
        }
    }

    private static void discardNow(Connection connection, List<Long> staged) throws SQLException {
        Database.inTransaction(connection, () -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM " + ConfigHandler.prefix + "segment_unit WHERE segment_id IN (SELECT id FROM " + ConfigHandler.prefix + "segment WHERE compacted = "
                        + WRITING + " AND id IN (" + join(staged) + "))");
                statement.executeUpdate("DELETE FROM " + ConfigHandler.prefix + "segment WHERE compacted = " + WRITING + " AND id IN (" + join(staged) + ")");
            }
        });
    }

    private static String join(List<Long> ids) {
        StringBuilder joined = new StringBuilder();
        for (Long id : ids) {
            if (joined.length() > 0) {
                joined.append(',');
            }
            joined.append(id.longValue());
        }
        return joined.toString();
    }

    private static void hold(long rows) {
        long held = HELD_ROWS.addAndGet(rows);
        PEAK_HELD_ROWS.accumulateAndGet(held, Math::max);
    }

    private static void release(long rows) {
        HELD_ROWS.addAndGet(-rows);
    }

    /**
     * Reads the rows of one stream in order, a group at a time: each of its segments in turn, each
     * segment's units in turn, and each unit's groups in turn. Holds one group of decoded rows, the
     * body of the unit they came from, and the text and blob bytes of that unit: its own share for a
     * compacted segment, or the whole payload frame of a segment written by packing, which holds at
     * most one packed segment's worth of them.
     */
    private static final class StreamCursor implements AutoCloseable {
        private final Connection connection;
        private final ColdRollupTask.LookupOrder order;
        private final List<Stored> segments;
        private final long[] rowIds = new long[ColdSegmentCodec.GROUP_ROWS];
        private final Object[][] rows = new Object[ColdSegmentCodec.GROUP_ROWS][];
        private final long[] key;
        private int segmentIndex = -1;
        private Stored segment;
        private ColdSegmentCodec.GroupReader reader;
        private int payloadSize;
        private int dictionaryId;
        private boolean sharesInUnits;
        private byte[] payloadFrame;
        private int group;
        private int unit = -1;
        private byte[] body;
        private byte[] payload;
        private int payloadOffset;
        private int size;
        private int position = -1;
        /** Rows decoded but not yet taken, which count towards what the merge holds. */
        private int untaken;
        private long segmentRows;

        private StreamCursor(Connection connection, ColdRollupTask.LookupOrder order, List<Stored> segments) {
            this.connection = connection;
            this.order = order;
            this.segments = segments;
            this.key = new long[order.keyLength()];
        }

        /**
         * Moves to the next row.
         *
         * @return false once every row of the stream has been read
         */
        private boolean next() throws SQLException {
            position++;
            while (position >= size) {
                if (!loadGroup()) {
                    return false;
                }
            }
            order.key(rowIds[position], rows[position], key);
            return true;
        }

        private long rowId() {
            return rowIds[position];
        }

        /**
         * @return the current row's values, which the cursor no longer holds
         */
        private Object[] take() {
            Object[] values = rows[position];
            rows[position] = null;
            untaken--;
            return values;
        }

        private boolean loadGroup() throws SQLException {
            while (true) {
                if (reader != null && group < reader.groupCount()) {
                    try {
                        int groupUnit = reader.unitOf(group);
                        if (groupUnit != unit) {
                            openUnit(groupUnit);
                        }
                        size = reader.decodeGroup(group, body, payload, payloadOffset, rowIds, rows);
                    }
                    catch (IllegalArgumentException malformed) {
                        throw new SQLException("Segment " + segment.id + " cannot be read for compaction", malformed);
                    }
                    group++;
                    position = 0;
                    untaken = untaken + size;
                    hold(size);
                    segmentRows = segmentRows + size;
                    return true;
                }
                if (reader != null) {
                    finishSegment();
                }
                segmentIndex++;
                if (segmentIndex >= segments.size()) {
                    size = 0;
                    position = 0;
                    return false;
                }
                openSegment(segments.get(segmentIndex));
            }
        }

        private void openSegment(Stored next) throws SQLException {
            segment = next;
            segmentRows = 0;
            group = 0;
            unit = -1;
            body = null;
            payload = null;
            try (PreparedStatement statement = connection.prepareStatement("SELECT scalars,payload,payload_size,dict_id FROM " + ConfigHandler.prefix + "segment WHERE id = ?")) {
                statement.setLong(1, next.id);
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        throw new SQLException("Segment " + next.id + " went missing while it was being compacted");
                    }
                    try {
                        reader = new ColdSegmentCodec.GroupReader(results.getBytes("scalars"));
                    }
                    catch (IllegalArgumentException malformed) {
                        throw new SQLException("Segment " + next.id + " cannot be read for compaction", malformed);
                    }
                    byte[] frame = results.getBytes("payload");
                    payloadSize = results.getInt("payload_size");
                    dictionaryId = results.getInt("dict_id");
                    sharesInUnits = frame == null && payloadSize > 0;
                    payloadFrame = frame == null ? null : SegmentDictionary.decompress(frame, payloadSize, dictionaryId, connection);
                    if (payloadFrame != null && payloadFrame.length != payloadSize) {
                        throw new SQLException("Segment " + next.id + " payload decompressed to the wrong length");
                    }
                }
            }
        }

        /**
         * Reads a unit's body and its share of the payload: from the unit's own row for a segment
         * that keeps them there, or from the segment's payload frame for one that does not.
         */
        private void openUnit(int next) throws SQLException {
            byte[] stored;
            byte[] storedShare;
            try (PreparedStatement statement = connection.prepareStatement("SELECT body,payload FROM " + ConfigHandler.prefix + "segment_unit WHERE segment_id = ? AND unit = ?")) {
                statement.setLong(1, segment.id);
                statement.setInt(2, next);
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        throw new SQLException("Segment " + segment.id + " is missing unit " + next);
                    }
                    stored = results.getBytes(1);
                    storedShare = results.getBytes(2);
                }
            }
            body = reader.openUnit(next, stored);

            int start = reader.payloadStart(next);
            int end = reader.payloadEnd(next, payloadSize);
            payloadOffset = start;
            if (payloadSize == 0 || end == start) {
                payload = new byte[0];
            }
            else if (!sharesInUnits) {
                payload = payloadFrame;
                payloadOffset = 0;
            }
            else {
                if (storedShare == null) {
                    throw new SQLException("Segment " + segment.id + " unit " + next + " holds no payload");
                }
                payload = SegmentDictionary.decompress(storedShare, end - start, dictionaryId, connection);
                if (payload.length != end - start) {
                    throw new SQLException("Segment " + segment.id + " unit " + next + " payload decompressed to the wrong length");
                }
            }
            unit = next;
        }

        /** Checks that a segment held exactly the rows it records, and lets go of it. */
        private void finishSegment() throws SQLException {
            if (segmentRows != segment.rowCount || segmentRows != reader.rowCount()) {
                throw new SQLException("Segment " + segment.id + " holds " + segmentRows + " rows but records " + segment.rowCount);
            }
            reader = null;
            body = null;
            payload = null;
            payloadFrame = null;
        }

        /** Checks that every segment of the stream was read to the end. */
        private void finish() throws SQLException {
            if (segmentIndex < segments.size() || reader != null) {
                throw new SQLException("Compaction stopped part way through segment " + (segment == null ? segments.get(0).id : segment.id));
            }
        }

        @Override
        public void close() {
            payloadFrame = null;
            release(untaken);
            untaken = 0;
        }
    }
}

package net.coreprotect.services;

import java.sql.Connection;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.ColdRollupTask;
import net.coreprotect.database.Database;
import net.coreprotect.database.SegmentCompactor;
import net.coreprotect.utility.ErrorReporter;

/**
 * Packs logged activity into compressed storage as it accumulates, rather than once a night.
 *
 * <p>
 * The live tables are a write buffer, not a window of recent history: as soon as a table has
 * logged a segment's worth of rows for one day they are packed, and the remainder of a day is packed
 * once the day is over. Lookups read the buffer and the segments together, so nobody can tell, or
 * needs to know, where a row currently is. Keeping the buffer small is also what keeps writes cheap:
 * the indexes on the live tables only ever cover a few thousand rows.
 * </p>
 *
 * <p>
 * Checking is cheap and done every {@link #INTERVAL_MILLIS}; packing only starts when there is a
 * whole segment to write, and gives way to purges, rollbacks, migrations and shutdown between
 * segments. SQLite only: the other backends compress their own storage.
 * </p>
 */
public final class StoragePackingService {

    /** How often to look for rows ready to be packed. */
    static final long INTERVAL_MILLIS = 30_000L;

    /**
     * Segments written in the older format rewritten per pass when there is nothing to pack. Passes
     * follow each other closely while such segments remain, so an upgraded server brings its whole
     * history up to date in minutes rather than hours, and every lookup over that history gets the
     * faster format; between segments logging still gets the database.
     */
    private static final int REPACK_PER_PASS = 16;

    /** The pause between passes while there is more to do. */
    private static final long BUSY_INTERVAL_MILLIS = 250L;

    /** Pause between segments, so logging always gets the database in between. */
    private static final long SEGMENT_PAUSE_MILLIS = 50L;

    private static final Object SLEEP = new Object();

    private static volatile Thread worker;
    private static volatile boolean stopRequested;
    private static volatile boolean working;

    private StoragePackingService() {
        throw new IllegalStateException("Service class");
    }

    /**
     * Starts packing in the background. Called once the plugin has finished loading.
     */
    public static synchronized void start() {
        if (worker != null && worker.isAlive()) {
            return;
        }

        stopRequested = false;
        Thread thread = new Thread(StoragePackingService::run, "CoreProtect-StoragePacking");
        thread.setDaemon(true);
        worker = thread;
        thread.start();
    }

    /**
     * Stops packing. A segment being written is finished first; the rest waits for the next start.
     */
    public static synchronized void stop() {
        stopRequested = true;
        worker = null;
        synchronized (SLEEP) {
            SLEEP.notifyAll();
        }
    }

    /**
     * @return true while a segment is being packed, so shutdown can wait for it
     */
    public static boolean isWorking() {
        return working;
    }

    /** The longest the packing waits after failing again and again. */
    private static final long MAXIMUM_BACKOFF_MILLIS = 30L * 60L * 1000L;

    /**
     * The failures reported in full since the last pass that succeeded, so a fault that persists is
     * reported once rather than every half minute.
     */
    private static final java.util.Set<String> REPORTED = new java.util.HashSet<>();

    private static void run() {
        boolean busy = false;
        int failures = 0;
        while (!stopRequested) {
            // After a failure the next pass waits longer each time, up to half an hour, so a fault
            // that has not been put right costs little and says so rarely.
            long wait = failures > 0 ? Math.min(MAXIMUM_BACKOFF_MILLIS, INTERVAL_MILLIS << Math.min(failures, 10)) : busy ? BUSY_INTERVAL_MILLIS : INTERVAL_MILLIS;
            synchronized (SLEEP) {
                try {
                    SLEEP.wait(wait);
                }
                catch (InterruptedException exception) {
                    return;
                }
            }
            if (stopRequested) {
                return;
            }

            busy = false;
            try {
                busy = packOnce() > 0;
                failures = 0;
                REPORTED.clear();
            }
            catch (InterruptedException exception) {
                // Something needed the database, or the server is stopping. The rows are packed on
                // a later pass.
            }
            catch (Exception exception) {
                failures++;
                if (REPORTED.add(exception.getClass().getName() + ": " + exception.getMessage())) {
                    ErrorReporter.report(exception);
                }
            }
        }
    }

    /**
     * Packs whatever is ready, if the database is free to do so, or rewrites a few segments of the
     * older format when nothing is, or compacts one run of a day that is over when neither is.
     *
     * @return how much was done: rows packed, segments rewritten or rows compacted, 0 when there
     *         was nothing to do
     * @throws Exception
     *             if packing fails or has to stop
     */
    static long packOnce() throws Exception {
        if (!ConfigHandler.databaseType.isSQLite() || mustYield()) {
            return 0;
        }

        Connection connection = Database.getConnection(false, 1000);
        if (connection == null) {
            return 0;
        }

        try {
            if (!ColdRollupTask.hasWork(connection)) {
                working = true;
                long rewritten = ColdRollupTask.repackOldSegments(connection, StoragePackingService::beforeSegment, REPACK_PER_PASS);
                if (rewritten > 0) {
                    return rewritten;
                }
                // With nothing to pack or rewrite, the days that are over are merged into a few
                // segments each, one run at a time, so a lookup about a place opens about one
                // segment per day rather than one for every few minutes anyone was there.
                return SegmentCompactor.compactNext(connection, StoragePackingService::beforeSegment);
            }
            working = true;
            return ColdRollupTask.rollUpIfIdle(connection, StoragePackingService::beforeSegment);
        }
        finally {
            working = false;
            try {
                connection.close();
            }
            catch (Exception exception) {
                ErrorReporter.report(exception);
            }
        }
    }

    private static void beforeSegment() throws InterruptedException {
        if (mustYield()) {
            throw new InterruptedException("Storage packing stopped");
        }
        Thread.sleep(SEGMENT_PAUSE_MILLIS);
    }

    private static boolean mustYield() {
        return stopRequested || !ConfigHandler.serverRunning || !ConfigHandler.databaseReachable || ConfigHandler.converterRunning
                || ConfigHandler.purgeRunning || ConfigHandler.migrationRunning || Consumer.isBackgroundPurgeRunning()
                || Consumer.isPersistenceHalted() || Consumer.isDatabaseReloadPaused() || !ConfigHandler.activeRollbacks.isEmpty();
    }
}

package net.coreprotect.services;

import java.sql.Connection;
import java.sql.Statement;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.BlobRecompressTask;
import net.coreprotect.database.ColdRollupTask;
import net.coreprotect.database.CompactProgress;
import net.coreprotect.database.Database;
import net.coreprotect.database.PurgeExecutor;
import net.coreprotect.database.PurgePolicy;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.EntitySpawnTracking;
import net.coreprotect.utility.ErrorReporter;

/**
 * Runs the automatic purge that keeps the database within the configured retention period.
 *
 * <p>
 * Automatic purging is always enabled; the {@code auto-purge} configuration option controls how
 * much history is kept, and {@code auto-purge-time} controls the time of day the purge runs.
 * Rows are deleted in place, in small batches with a short pause between them, so the database
 * is never duplicated and the server stays usable while the purge runs.
 * </p>
 */
public final class AutoPurgeService {

    /** Retention used when the configured value is missing or cannot be understood. */
    private static final long DEFAULT_RETENTION_SECONDS = 15552000L; // 180 days

    /** The shortest retention period a purge will honor, matching the manual purge command. */
    private static final long MINIMUM_RETENTION_SECONDS = 2592000L; // 30 days

    /** Rows removed per statement, followed by a pause so other database work can proceed. */
    private static final int BATCH_SIZE = 5000;

    /** Pause between batches, in milliseconds. */
    private static final long BATCH_PAUSE_MILLIS = 500L;

    private static final DateTimeFormatter SCHEDULE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ROOT);

    private static volatile Thread worker;
    private static volatile boolean stopRequested;

    private AutoPurgeService() {
        throw new IllegalStateException("Service class");
    }

    /**
     * Starts the automatic purge scheduler. Called once the plugin has finished loading.
     */
    public static synchronized void start() {
        if (worker != null && worker.isAlive()) {
            return;
        }

        stopRequested = false;
        Thread thread = new Thread(AutoPurgeService::run, "CoreProtect-AutoPurge");
        thread.setDaemon(true);
        worker = thread;
        thread.start();
    }

    /**
     * Stops the automatic purge scheduler and asks any purge in progress to stop at the next
     * batch boundary. A partially completed purge simply continues at the next scheduled run.
     */
    public static synchronized void stop() {
        stopRequested = true;
        Thread thread = worker;
        worker = null;
        if (thread != null) {
            thread.interrupt();
        }
    }

    /**
     * Asks a purge that is currently running to stop, without stopping the scheduler. Used when
     * a manual purge needs the database.
     */
    public static void requestCancel() {
        Thread thread = worker;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static void run() {
        try {
            LocalDateTime nextRun = nextRunTime(LocalDateTime.now());
            announceSchedule(nextRun);

            while (!stopRequested && ConfigHandler.serverRunning) {
                Thread.sleep(1000L);

                if (LocalDateTime.now().isBefore(nextRun)) {
                    continue;
                }

                try {
                    purge();
                }
                catch (InterruptedException e) {
                    // A purge that stops for a manual purge or a busy database is not a failure;
                    // the scheduler keeps running and the work resumes at the next scheduled run.
                    if (stopRequested || !ConfigHandler.serverRunning) {
                        throw e;
                    }
                }
                catch (Exception e) {
                    ErrorReporter.report(e);
                }

                Thread.interrupted(); // clear a cancellation left by a stopped purge
                nextRun = nextRunTime(LocalDateTime.now().plusMinutes(1));
                announceSchedule(nextRun);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void purge() throws Exception {
        long retention = retentionSeconds();
        long cutoff = (System.currentTimeMillis() / 1000L) - retention;

        Consumer.OperationStartResult startResult = Consumer.claimBackgroundPurge();
        if (startResult != Consumer.OperationStartResult.STARTED) {
            Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_SKIPPED));
            return;
        }

        long removed = 0;
        try {
            Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_STARTED, describeRetention(retention)));

            if (ConfigHandler.databaseType.isClickHouse()) {
                if (!Config.getGlobal().DATABASE_LOCK) {
                    // ClickHouse purges require exclusive access to the shared namespace.
                    Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_SKIPPED));
                    return;
                }
                removed = Database.purgeClickHouse(0, cutoff, 0, Collections.emptyList(), false);
            }
            else {
                removed = purgeRelational(cutoff);
            }

            EntitySpawnTracking.invalidateDatabaseVerification();
            ConfigHandler.autoPurgeRowsPurged.addAndGet(removed);
            rollUpColdStorage();
            Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_COMPLETED, NumberFormat.getInstance().format(removed), (removed == 1 ? Selector.FIRST : Selector.SECOND)));
        }
        catch (InterruptedException e) {
            ConfigHandler.autoPurgeRowsPurged.addAndGet(removed);
            throw e;
        }
        catch (Exception e) {
            ConfigHandler.autoPurgeRowsPurged.addAndGet(removed);
            if (!isCancellation(e)) {
                throw e;
            }
        }
        finally {
            Consumer.releaseBackgroundPurge();
        }
    }

    /**
     * Packs activity that has aged out of the hot window into compressed storage. Runs straight
     * after the purge, while the background purge claim is still held, so it never competes with a
     * manual purge or a rollback.
     *
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    private static void rollUpColdStorage() throws Exception {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return;
        }

        Connection connection = Database.getConnection(false, 1000);
        if (connection == null) {
            return;
        }

        try {
            long sealed = ColdRollupTask.rollUp(connection, AutoPurgeService::awaitNextBatch);
            if (sealed > 0) {
                Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.COLD_STORAGE_ROLLED_UP, NumberFormat.getInstance().format(sealed), (sealed == 1 ? Selector.FIRST : Selector.SECOND)));
            }
        }
        finally {
            try {
                connection.close();
            }
            catch (Exception e) {
                ErrorReporter.report(e);
            }
        }
    }

    private static long purgeRelational(long cutoff) throws Exception {
        Connection connection = Database.getConnection(false, 1000);
        if (connection == null) {
            Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_SKIPPED));
            return 0;
        }

        long removed = 0;
        try {
            PurgeExecutor.StatementFactory factory = Connection::prepareStatement;
            PurgeExecutor.BatchCallback callback = AutoPurgeService::awaitNextBatch;

            removed = removed + PurgeExecutor.purgeColdSegments(connection, cutoff, factory, callback);

            for (String table : PurgePolicy.getPurgeableTables()) {
                removed = removed + PurgeExecutor.purgeTable(connection, table, 0, cutoff, 0, null, BATCH_SIZE, factory, callback);
            }

            removed = removed + PurgeExecutor.removeOrphanedRows(connection, factory, callback);

            // Pack aged rows away and make sure every segment carries the counts lookups rely on.
            ColdRollupTask.repairRowIds(connection, AutoPurgeService::awaitNextBatch);
            ColdRollupTask.rollUp(connection, AutoPurgeService::awaitNextBatch);
            ColdRollupTask.backfillStatistics(connection, AutoPurgeService::awaitNextBatch);
            // Entity data is never packed into a segment, because it is read a row at a time. It is
            // compressed where it lies instead.
            BlobRecompressTask.run(connection, AutoPurgeService::awaitNextBatch);

            if (ConfigHandler.databaseType.isSQLite()) {
                // Deleted pages stay in the database file and are reused by future writes;
                // truncating the write-ahead log releases the space it grew to during the purge.
                try (Statement statement = connection.createStatement()) {
                    Database.performCheckpoint(statement, ConfigHandler.databaseType);
                }
            }
            else if (ConfigHandler.databaseType.isDuckDB()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("CHECKPOINT");
                }
            }
        }
        finally {
            try {
                connection.close();
            }
            catch (Exception e) {
                ErrorReporter.report(e);
            }
        }

        return removed;
    }

    /**
     * Pauses between batches and stops the purge whenever the database is needed for something
     * else. The purge resumes from the same cutoff at the next scheduled run, so stopping early
     * never loses progress.
     *
     * @throws InterruptedException
     *             if the purge should stop
     */
    /** Seconds between saying how far along the nightly work is. */
    private static final long PROGRESS_INTERVAL = 20;

    private static long nextProgress;

    private static void awaitNextBatch() throws InterruptedException {
        if (stopRequested || !ConfigHandler.serverRunning || ConfigHandler.converterRunning || ConfigHandler.purgeRunning || ConfigHandler.migrationRunning || Consumer.isPersistenceHalted() || Consumer.isDatabaseReloadPaused()) {
            throw new InterruptedException("Automatic purge stopped");
        }

        reportProgress();
        Thread.sleep(BATCH_PAUSE_MILLIS);
    }

    /**
     * Says how far along the nightly work is, now and then. It runs unattended and can take a long
     * while on a large database, so a line in the console is the only sign it is still going.
     */
    private static void reportProgress() {
        long now = System.currentTimeMillis() / 1000L;
        if (now < nextProgress) {
            return;
        }
        nextProgress = now + PROGRESS_INTERVAL;

        String progress = CompactProgress.line();
        if (progress != null) {
            Chat.console(Phrase.build(Phrase.COMPACT_PROGRESS, progress));
        }
    }

    private static boolean isCancellation(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void announceSchedule(LocalDateTime nextRun) {
        Chat.sendConsoleMessage(Color.GREY + "[CoreProtect] " + Phrase.build(Phrase.AUTO_PURGE_SCHEDULED, SCHEDULE_FORMAT.format(nextRun), describeRetention(retentionSeconds())));
    }

    private static LocalDateTime nextRunTime(LocalDateTime from) {
        LocalTime runTime = configuredRunTime();
        LocalDateTime candidate = from.toLocalDate().atTime(runTime);
        if (!candidate.isAfter(from)) {
            candidate = candidate.plusDays(1);
        }
        return candidate;
    }

    private static LocalTime configuredRunTime() {
        String configured = Config.getGlobal().AUTO_PURGE_TIME;
        if (configured == null || configured.trim().isEmpty()) {
            return LocalTime.MIDNIGHT;
        }

        try {
            return LocalTime.parse(configured.trim());
        }
        catch (Exception e) {
            return LocalTime.MIDNIGHT;
        }
    }

    /**
     * Reads the configured retention period. Automatic purging is always enabled, so a missing,
     * disabled, or unreadable value falls back to the default retention.
     *
     * @return the retention period in seconds, never below the 30 day minimum
     */
    static long retentionSeconds() {
        long parsed = parseRetention(Config.getGlobal().AUTO_PURGE);
        if (parsed <= 0) {
            parsed = DEFAULT_RETENTION_SECONDS;
        }
        return Math.max(MINIMUM_RETENTION_SECONDS, parsed);
    }

    /**
     * Parses a retention value such as {@code 180d}, {@code 12w}, or {@code 6mo}. A plain number
     * is read as a number of days.
     *
     * @param value
     *            the configured value
     * @return the retention in seconds, or 0 if the value cannot be understood
     */
    static long parseRetention(String value) {
        if (value == null) {
            return 0;
        }

        String input = value.trim().toLowerCase(Locale.ROOT).replace(" ", "");
        if (input.isEmpty() || input.equals("false") || input.equals("none") || input.equals("off")) {
            return 0;
        }

        long total = 0;
        long amount = 0;
        boolean hasAmount = false;
        boolean hasUnit = false;

        for (int i = 0; i < input.length(); i++) {
            char character = input.charAt(i);
            if (character >= '0' && character <= '9') {
                amount = (amount * 10) + (character - '0');
                hasAmount = true;
                continue;
            }

            if (!hasAmount) {
                return 0;
            }

            long unit;
            if (character == 'm' && input.startsWith("mo", i)) {
                unit = 2592000L; // month
                i++;
            }
            else if (character == 'y') {
                unit = 31536000L;
            }
            else if (character == 'w') {
                unit = 604800L;
            }
            else if (character == 'd') {
                unit = 86400L;
            }
            else if (character == 'h') {
                unit = 3600L;
            }
            else if (character == 'm') {
                unit = 60L;
            }
            else if (character == 's') {
                unit = 1L;
            }
            else {
                return 0;
            }

            total = total + (amount * unit);
            amount = 0;
            hasAmount = false;
            hasUnit = true;
        }

        if (hasAmount && !hasUnit) {
            return amount * 86400L; // a plain number is a number of days
        }
        if (hasAmount) {
            return 0;
        }

        return total;
    }

    private static String describeRetention(long seconds) {
        Duration duration = Duration.ofSeconds(seconds);
        long days = duration.toDays();
        if (days > 0) {
            return days + (days == 1 ? " day" : " days");
        }
        long hours = duration.toHours();
        return hours + (hours == 1 ? " hour" : " hours");
    }
}

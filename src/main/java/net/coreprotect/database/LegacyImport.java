package net.coreprotect.database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.patch.Patch;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.VersionUtils;

/**
 * Imports a database written by upstream CoreProtect into the version 2 hot and cold layout.
 *
 * <p>
 * A file named {@code old.db} in the plugin folder is treated as a database to import. The version 1
 * and version 2 live tables have the same columns, so nothing about a row's values has to be
 * converted: rows are copied with their row ids intact and the roll-up then compresses them. Row ids
 * have to survive because they are what the data refers to itself by, from
 * {@code entity_spawn.block_rowid} to the identifier ids in the map tables, which is also why the
 * import only runs into a database that has no activity of its own yet.
 * </p>
 *
 * <p>
 * The import runs in two parts. The tables the cold store never touches are small, are read into
 * memory caches while the server starts, and are copied before those caches load; if they arrived
 * later, live logging would mint new identifier ids for materials the imported rows already refer to
 * under different ones. The nine large activity tables are then filled in the background while the
 * server runs.
 * </p>
 *
 * <p>
 * Logging stays on throughout. Before an activity table is filled, its single highest numbered
 * legacy row is copied first, which leaves the live table's highest row id at the top of the legacy
 * range, so every row the server writes from then on lands above everything still to be imported and
 * no row id is ever handed out twice.
 * </p>
 */
public final class LegacyImport {

    /** The file the import reads, in the plugin folder. */
    private static final String SOURCE_FILE = "old.db";

    /** The schema name the source database is attached under. */
    private static final String LEGACY = "legacy";

    /**
     * Rows copied per transaction. Every batch is one transaction, and an uncommitted transaction
     * lives in the write ahead log, so this also bounds how much free disk the import needs beyond
     * the data itself.
     */
    private static final int BATCH_ROWS = 20000;

    private static final long SECONDS_PER_DAY = 86400L;

    /** Seconds between progress lines in the console. */
    private static final long PROGRESS_INTERVAL = 15;

    /** How far down from the newest row the search for a readable one goes on a damaged source. */
    private static final int FENCE_ATTEMPTS = 4096;

    private static final DateTimeFormatter ARCHIVE_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT);

    /**
     * The identifier tables, copied before the caches load. Every one of them is small: they hold one
     * row per world, material, block state and player, not one per event.
     */
    private static final List<String> REFERENCE_TABLES = Arrays.asList("art_map", "blockdata_map", "entity_map",
            "material_map", "world", "user", "username_log", "skull", "version");

    /**
     * Tables that are never rolled into segments but are far too large to hold up the server while
     * they are copied. Nothing reads them while the server starts, so they are filled alongside the
     * activity tables and held open by the same row id fence.
     */
    private static final List<String> DEFERRED_TABLES = Arrays.asList("entity", "entity_spawn");

    /** Every table the import may write, used for the check that the target starts out empty. */
    private static final List<String> ALL_TABLES = new ArrayList<>();

    static {
        ALL_TABLES.addAll(REFERENCE_TABLES);
        ALL_TABLES.addAll(DEFERRED_TABLES);
        ALL_TABLES.addAll(SQLiteColdIndex.getSegmentedTables());
    }

    /** The prefix the source database uses, which need not be the one this server is configured with. */
    private static volatile String sourcePrefix;

    /** Rows the source could not be read for, across the whole import. */
    private static volatile long skippedRows;

    /** Set once the reference tables are in place and the activity tables still have to be filled. */
    private static volatile boolean activityPending;

    /** Set while an import is running, for {@code /co status} and for the commands it blocks. */
    private static volatile boolean running;

    /**
     * Set once the server is up and the import can be stopped by a shutdown. The identifier tables
     * are copied before the server is marked as running, so a cancellation check there would stop the
     * import before it started.
     */
    private static volatile boolean cancellable;

    /** The table being copied and how far through it the import is, for {@code /co status}. */
    private static volatile String progressTable;
    private static volatile int progressPercent;

    private LegacyImport() {
        throw new IllegalStateException("Import class");
    }

    /**
     * @return a line describing what the import is doing, or null when no import is running
     */
    public static String getProgress() {
        String table = progressTable;
        if (!running || table == null) {
            return null;
        }
        return Phrase.build(Phrase.IMPORT_PROGRESS, Color.WHITE, table, String.valueOf(progressPercent));
    }

    /**
     * @return true while an import is running
     */
    public static boolean isRunning() {
        return running;
    }

    /**
     * @return the number of rows the source could not be read for during the current import
     */
    public static long getSkippedRows() {
        return skippedRows;
    }

    /**
     * Copies the tables that have to be in place before the identifier caches load.
     *
     * <p>
     * Called during start-up, before worlds, materials and players are read into memory. Everything
     * that would stop the import is checked here, so that a refusal happens before anything has been
     * written and the source file is left exactly as it was found.
     * </p>
     *
     * @return true when an import has started and the activity tables still have to be filled
     */
    public static boolean importReferenceTables() {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return false;
        }

        Path source = Paths.get(ConfigHandler.path + SOURCE_FILE);
        if (!Files.exists(source)) {
            return false;
        }

        activityPending = false;
        running = false;
        progressTable = null;
        progressPercent = 0;
        skippedRows = 0;

        Connection connection = null;
        try {
            connection = openConnection();
            attach(connection, source);

            String prefix = detectPrefix(connection);
            if (prefix == null || isVersionTwo(connection, prefix)) {
                Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_SOURCE_UNSUPPORTED, SOURCE_FILE));
                return false;
            }
            sourcePrefix = prefix;

            String state = readMarker(connection, "import_state");
            if ("done".equals(state)) {
                // A previous import copied everything but could not finish tidying up. Nothing is
                // left to read from the source, so it is set aside now and the markers cleared.
                Path archived = archive(source);
                clearMarkers(connection);
                Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_COMPLETED, "0", Selector.SECOND, archived.getFileName().toString()));
                return false;
            }

            String fingerprint = fingerprint(connection, prefix);
            String recorded = readMarker(connection, "import_source");
            boolean resuming = "running".equals(state);

            if (resuming) {
                if (!fingerprint.equals(recorded)) {
                    Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_SOURCE_CHANGED, SOURCE_FILE));
                    return false;
                }
            }
            else {
                if (!targetIsEmpty(connection)) {
                    Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_TARGET_NOT_EMPTY, SOURCE_FILE));
                    return false;
                }
                writeMarker(connection, "import_source", fingerprint);
                writeMarker(connection, "import_state", "running");
                Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_STARTED, SOURCE_FILE));
            }

            ConfigHandler.migrationRunning = true;
            running = true;
            cancellable = false;
            progressTable = "identifiers";
            warnAboutFreeSpace(source);

            for (String table : REFERENCE_TABLES) {
                copyTable(connection, table, false, false);
            }

            activityPending = true;
            return true;
        }
        catch (Exception exception) {
            ConfigHandler.migrationRunning = false;
            running = false;
            if (!reportCorruption(connection, exception)) {
                Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_FAILED));
            }
            ErrorReporter.report(exception);
            return false;
        }
        finally {
            close(connection);
        }
    }

    /**
     * Fills the activity tables, either in the background or, when the imported data still has to be
     * upgraded, before the server finishes starting.
     *
     * <p>
     * Upstream's upgrade scripts rewrite rows in the live tables, so when they have to run the rows
     * cannot be compressed on the way in: the import finishes first and the packing waits for the
     * nightly roll-up or for {@code /co compact}. When there is nothing to upgrade the rows are
     * sealed into segments as they arrive, and the database never holds an uncompressed copy of the
     * whole history.
     * </p>
     *
     * @param connection
     *            a connection used only to decide whether an upgrade is pending
     */
    public static void importActivityTables(Connection connection) {
        if (!activityPending) {
            return;
        }
        activityPending = false;

        boolean upgradePending = upgradePending(connection);
        if (upgradePending) {
            Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_PATCHES_PENDING, SOURCE_FILE));
            runActivityImport(false);
            return;
        }

        Thread worker = new Thread(() -> runActivityImport(true), "CoreProtect-Import");
        worker.start();
    }

    /**
     * @param connection
     *            an open connection to the database being imported into
     * @return true when upstream's upgrade scripts still have to run against the imported rows
     */
    private static boolean upgradePending(Connection connection) {
        try {
            if (ConfigHandler.EDITION_BRANCH.contains("-dev")) {
                return false;
            }
            Integer[] lastVersion = Patch.getDatabaseVersion(connection, true);
            return lastVersion[0] > 0 && VersionUtils.newVersion(lastVersion, VersionUtils.getInternalPluginVersion());
        }
        catch (Exception exception) {
            ErrorReporter.report(exception);
            return false;
        }
    }

    static void runActivityImport(boolean seal) {
        Connection connection = null;
        long imported = 0;
        try {
            Path source = Paths.get(ConfigHandler.path + SOURCE_FILE);
            connection = openConnection();
            attach(connection, source);

            cancellable = true;
            // The two large tables the cold store never touches go first: the activity tables refer
            // to entity spawns by row id, so nothing should be looked up before they are all there.
            for (String table : DEFERRED_TABLES) {
                imported = imported + copyTable(connection, table, true, false);
            }
            for (String table : SQLiteColdIndex.getSegmentedTables()) {
                imported = imported + copyTable(connection, table, true, seal);
            }

            writeMarker(connection, "import_state", "done");
            detach(connection);

            Path archived = archive(source);
            clearMarkers(connection);

            running = false;
            cancellable = false;
            ConfigHandler.migrationRunning = false;
            Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_COMPLETED,
                    NumberFormat.getInstance().format(imported), (imported == 1 ? Selector.FIRST : Selector.SECOND),
                    archived.getFileName().toString()));
            if (imported == 0) {
                Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_NOTHING_TO_DO, SOURCE_FILE));
            }
            if (skippedRows > 0) {
                Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_SKIPPED_TOTAL,
                        NumberFormat.getInstance().format(skippedRows), (skippedRows == 1 ? Selector.FIRST : Selector.SECOND), SOURCE_FILE));
            }
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            running = false;
            cancellable = false;
            ConfigHandler.migrationRunning = false;
            Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_STOPPED));
        }
        catch (Exception exception) {
            running = false;
            cancellable = false;
            ConfigHandler.migrationRunning = false;
            if (!reportCorruption(connection, exception)) {
                Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_FAILED));
            }
            ErrorReporter.report(exception);
        }
        finally {
            close(connection);
        }
    }

    /**
     * Copies one table in batches, recording how far it has reached after each one.
     *
     * <p>
     * Every batch is a transaction of its own. A single transaction over a whole table would need as
     * much free disk again as the table itself before anything could be committed, and would have to
     * start over from nothing if it were interrupted; batches bound both. What has been copied is
     * recorded as it goes, so a restart carries on from there rather than copying rows that are
     * already in place.
     * </p>
     *
     * @param connection
     *            an open connection with the source database attached
     * @param table
     *            an unprefixed table name
     * @param useFence
     *            whether to hold the row id space open first, for tables the server writes to while
     *            the import runs
     * @param seal
     *            whether the copied rows are packed into segments as they arrive
     * @return the number of rows copied
     */
    private static long copyTable(Connection connection, String table, boolean useFence, boolean seal) throws Exception {
        Columns columns = columns(connection, table);
        if (columns == null) {
            return 0;
        }

        // Rows are copied up to but not including the ceiling: for a fenced table that is the fence
        // row, which is already in place, and otherwise it is one past the last row there is.
        long ceiling;
        if (useFence) {
            ceiling = placeFence(connection, table);
        }
        else {
            long highest = maximumRowId(connection, table);
            ceiling = highest == 0 ? 0 : highest + 1;
        }
        if (ceiling == 0) {
            return 0;
        }

        String frontierKey = "import_" + table + "_rowid";
        String recordedFrontier = readMarker(connection, frontierKey);
        long frontier = recordedFrontier == null ? 0 : Long.parseLong(recordedFrontier);
        long copied = 0;
        long nextReport = 0;

        long last = ceiling - 1;
        while (frontier < last) {
            checkCancelled();

            // The batch is a span of row ids rather than a count of rows, so working out where it
            // ends needs no reading. A source with unreadable pages cannot be asked what is in it.
            long batchEnd = Math.min(frontier + BATCH_ROWS, last);

            long before = skippedRows;
            copied = copied + copySalvaging(connection, table, columns, frontier, batchEnd);
            if (skippedRows > before) {
                Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_SKIPPED_ROWS,
                        NumberFormat.getInstance().format(skippedRows - before),
                        ((skippedRows - before) == 1 ? Selector.FIRST : Selector.SECOND), table));
            }
            frontier = batchEnd;
            writeMarker(connection, frontierKey, Long.toString(frontier));
            checkpoint(connection);

            if (seal) {
                ColdRollupTask.sealTable(connection, table, sealBefore(), frontier, LegacyImport::checkCancelled);
            }

            int percent = (int) ((frontier * 100L) / last);
            progressTable = table;
            progressPercent = percent;
            long now = System.currentTimeMillis() / 1000L;
            if (now >= nextReport) {
                if (nextReport > 0) {
                    Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_PROGRESS, table, String.valueOf(percent)));
                }
                nextReport = now + PROGRESS_INTERVAL;
            }
        }

        return copied;
    }

    /**
     * Copies a range, working around parts of the source that cannot be read.
     *
     * <p>
     * A damaged database is damaged in places: some pages are unreadable and the rest are fine. Rather
     * than give up on the whole import, a range that fails is split in half and each half tried on its
     * own, down to single rows. Only the rows that genuinely cannot be read are given up, and how many
     * were lost is reported rather than passed over.
     * </p>
     *
     * <p>
     * Damage to the database being written to is a different matter and is not worked around: writing
     * around a fault there would spread it. The two are told apart by reading the same range back from
     * the source, which touches the same pages the copy did.
     * </p>
     *
     * @return the number of rows copied
     */
    private static long copySalvaging(Connection connection, String table, Columns columns, long after, long through) throws SQLException {
        try {
            return copyRange(connection, table, columns, after, through);
        }
        catch (SQLException exception) {
            if (!isCorruption(exception) || isReadable(connection, table, after, through)) {
                throw exception;
            }

            if (through - after <= 1) {
                skippedRows = skippedRows + 1;
                return 0;
            }

            long middle = after + ((through - after) / 2);
            return copySalvaging(connection, table, columns, after, middle)
                    + copySalvaging(connection, table, columns, middle, through);
        }
    }

    /**
     * @return true when the source can read back every row of a range, so the fault lies elsewhere
     */
    private static boolean isReadable(Connection connection, String table, long after, long through) {
        // Every column is selected so that the row bodies are read, not just the row ids that index
        // them: a damaged page is only noticed by something that actually reads it.
        String query = "SELECT COUNT(*) FROM (SELECT * FROM " + LEGACY + "." + sourcePrefix + table
                + " WHERE rowid > ? AND rowid <= ?)";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, after);
            statement.setLong(2, through);
            try (ResultSet results = statement.executeQuery()) {
                return results.next();
            }
        }
        catch (SQLException exception) {
            return false;
        }
    }

    private static boolean isCorruption(Exception exception) {
        String message = exception.getMessage();
        return message != null && (message.contains("malformed") || message.contains("SQLITE_CORRUPT"));
    }

    /**
     * Returns the pages a committed batch no longer needs to the database file.
     *
     * <p>
     * Committed transactions sit in the write ahead log until something moves them into the database
     * itself. Over an import of millions of rows that log would otherwise grow to the size of
     * everything copied, on top of the copy, which is free space the server may not have.
     * </p>
     */
    private static void checkpoint(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery("PRAGMA wal_checkpoint(PASSIVE)").close();
        }
        catch (SQLException exception) {
            // Nothing to check point, or another connection is reading. The next batch tries again.
        }
    }

    /**
     * Copies the single highest numbered legacy row of a table, before any other row of it.
     *
     * <p>
     * SQLite gives a new row the highest row id in the table plus one. Once this row is in place the
     * highest row id is the top of the legacy range, so every row the server writes from here on is
     * numbered above the whole import and can never be given a row id that a row still waiting to be
     * copied already owns. The row stays where it is until the table is finished; the roll-up is told
     * not to seal it, because sealing would delete it from the live table and hand its row id back
     * out again.
     * </p>
     *
     * @param connection
     *            an open connection with the source database attached
     * @param table
     *            an unprefixed table name
     * @return the row id the fence sits at, or 0 when the source has nothing in that table
     */
    static long placeFence(Connection connection, String table) throws SQLException {
        String fenceKey = "import_" + table + "_fence";
        String recorded = readMarker(connection, fenceKey);
        if (recorded != null) {
            return Long.parseLong(recorded);
        }

        Columns columns = columns(connection, table);
        long highest = columns == null ? 0 : maximumRowId(connection, table);
        if (highest == 0) {
            writeMarker(connection, fenceKey, "0");
            return 0;
        }

        // Usually the very top row. On a damaged source that row may be one of the unreadable ones,
        // so the search walks down until a row can be copied. Nothing is lost by settling lower: the
        // row ids passed over hold no imported row, so a row the server writes later may take one.
        long fence = 0;
        for (long candidate = highest; candidate > 0 && (highest - candidate) < FENCE_ATTEMPTS; candidate--) {
            try {
                if (copyRange(connection, table, columns, candidate - 1, candidate) > 0) {
                    fence = candidate;
                    break;
                }
            }
            catch (SQLException exception) {
                if (!isCorruption(exception) || isReadable(connection, table, candidate - 1, candidate)) {
                    throw exception;
                }
                skippedRows = skippedRows + 1;
            }
        }
        if (fence == 0) {
            throw new SQLException("Unable to read the newest rows of " + table + " from " + SOURCE_FILE);
        }

        writeMarker(connection, fenceKey, Long.toString(fence));
        writeMarker(connection, "import_" + table + "_rowid", "0");
        return fence;
    }

    /**
     * Copies a range of row ids in one transaction, so an interrupted import leaves whole batches
     * behind rather than a partly written one.
     *
     * @return the number of rows copied
     */
    private static long copyRange(Connection connection, String table, Columns columns, long after, long through) throws SQLException {
        String sql = "INSERT INTO main." + ConfigHandler.prefix + table + " (" + columns.target + ") SELECT " + columns.source
                + " FROM " + LEGACY + "." + sourcePrefix + table + " WHERE rowid > ? AND rowid <= ?";

        long[] copied = new long[1];
        Database.inTransaction(connection, () -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, after);
                statement.setLong(2, through);
                copied[0] = statement.executeUpdate();
            }
        });
        return copied[0];
    }

    /** The column list a copy reads and the one it writes, which differ when the source is older. */
    private static final class Columns {
        private final String target;
        private final String source;

        private Columns(String target, String source) {
            this.target = target;
            this.source = source;
        }
    }

    /**
     * Works out which columns to copy.
     *
     * <p>
     * Older CoreProtect databases have fewer columns than this one writes, so the copy uses the
     * columns both sides have and leaves the rest at their defaults. The row id is always copied,
     * either as the table's integer primary key or, when it has none, as {@code rowid} itself.
     * </p>
     *
     * @return the columns to copy, or null when the source has no such table
     */
    private static Columns columns(Connection connection, String table) throws SQLException {
        List<String> targetColumns = new ArrayList<>();
        String key = null;
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA main.table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                String name = results.getString("name");
                targetColumns.add(name);
                String type = results.getString("type");
                if (results.getInt("pk") == 1 && type != null && type.toUpperCase(Locale.ROOT).contains("INT")) {
                    key = name;
                }
            }
        }
        if (targetColumns.isEmpty()) {
            return null;
        }

        Set<String> sourceColumns = new LinkedHashSet<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA " + LEGACY + ".table_info(" + sourcePrefix + table + ")")) {
            while (results.next()) {
                sourceColumns.add(results.getString("name").toLowerCase(Locale.ROOT));
            }
        }
        if (sourceColumns.isEmpty()) {
            Chat.console(Color.GREY + "[CoreProtect] " + SOURCE_FILE + " has no " + table + " table; skipping it.");
            return null;
        }

        StringBuilder target = new StringBuilder();
        StringBuilder source = new StringBuilder();
        if (key == null) {
            // A plain row id table: the row id is not one of the columns, so it is named explicitly.
            target.append("rowid");
            source.append("rowid");
        }

        for (String column : targetColumns) {
            boolean isKey = column.equals(key);
            if (!isKey && !sourceColumns.contains(column.toLowerCase(Locale.ROOT))) {
                continue;
            }
            if (target.length() > 0) {
                target.append(',');
                source.append(',');
            }
            target.append(column);
            // A key the source does not name is still its row id, under whatever name it used.
            source.append(isKey && !sourceColumns.contains(column.toLowerCase(Locale.ROOT)) ? "rowid" : column);
        }

        return new Columns(target.toString(), source.toString());
    }

    /**
     * @return the cutoff the import seals below, matching the nightly roll-up's rule
     */
    private static long sealBefore() {
        long now = System.currentTimeMillis() / 1000L;
        return Math.min(now - Config.getGlobal().HOT_WINDOW_SECONDS, (now / SECONDS_PER_DAY) * SECONDS_PER_DAY);
    }

    /**
     * Stops the import when the server is going down. Shutdown waits for the import to finish, so it
     * has to notice promptly; what has been copied stays, and the rest resumes on the next start.
     */
    private static void checkCancelled() throws InterruptedException {
        if (cancellable && !ConfigHandler.serverRunning) {
            throw new InterruptedException("Server is shutting down");
        }
    }

    private static long maximumRowId(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT MAX(rowid) FROM " + LEGACY + "." + sourcePrefix + table)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /**
     * Describes the source database by its contents rather than by its file, because attaching it
     * can check-point a write-ahead log and change the file itself. An unfinished import will only
     * resume against the same data it started with.
     */
    private static String fingerprint(Connection connection, String prefix) throws SQLException {
        StringBuilder fingerprint = new StringBuilder();
        try (Statement statement = connection.createStatement()) {
            for (String table : ALL_TABLES) {
                if (fingerprint.length() > 0) {
                    fingerprint.append(',');
                }
                fingerprint.append(table).append(':');
                try (ResultSet results = statement.executeQuery("SELECT MIN(rowid),MAX(rowid) FROM " + LEGACY + "." + prefix + table)) {
                    if (results.next()) {
                        fingerprint.append(results.getLong(1)).append('-').append(results.getLong(2));
                    }
                }
                catch (SQLException exception) {
                    fingerprint.append("none");
                }
            }
        }
        return fingerprint.toString();
    }

    /**
     * @return true when the database being imported into holds no rows of its own
     */
    private static boolean targetIsEmpty(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String table : ALL_TABLES) {
                try (ResultSet results = statement.executeQuery("SELECT 1 FROM main." + ConfigHandler.prefix + table + " LIMIT 1")) {
                    if (results.next()) {
                        return false;
                    }
                }
            }
            try (ResultSet results = statement.executeQuery("SELECT 1 FROM main." + ConfigHandler.prefix + "segment LIMIT 1")) {
                if (results.next()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Finds the table prefix the source database uses, which is not necessarily the one configured
     * on this server.
     *
     * @return the prefix, or null when the file does not look like a CoreProtect database
     */
    private static String detectPrefix(Connection connection) throws SQLException {
        List<String> candidates = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT name FROM " + LEGACY + ".sqlite_master WHERE type='table' AND name LIKE '%block'")) {
            while (results.next()) {
                candidates.add(results.getString(1));
            }
        }

        for (String candidate : candidates) {
            if (candidate.equals(ConfigHandler.prefix + "block")) {
                return ConfigHandler.prefix;
            }
        }
        if (candidates.size() == 1) {
            String name = candidates.get(0);
            return name.substring(0, name.length() - "block".length());
        }
        return null;
    }

    /**
     * @return true when the source database already uses this fork's layout, which cannot be merged
     */
    private static boolean isVersionTwo(Connection connection, String prefix) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT name FROM " + LEGACY + ".sqlite_master WHERE type='table' AND name='" + prefix + "schema' LIMIT 1")) {
            return results.next();
        }
    }

    private static String readMarker(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT value FROM main." + ConfigHandler.prefix + "schema WHERE name=?")) {
            statement.setString(1, name);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? results.getString(1) : null;
            }
        }
    }

    private static void writeMarker(Connection connection, String name, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO main." + ConfigHandler.prefix + "schema (name, value) VALUES (?, ?)")) {
            statement.setString(1, name);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    private static void clearMarkers(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM main." + ConfigHandler.prefix + "schema WHERE name LIKE 'import\\_%' ESCAPE '\\'");
        }
    }

    /**
     * Warns when there may not be room for the import.
     *
     * <p>
     * The copy is a second copy of the data: the source stays where it is until the import finishes,
     * and the rows only shrink once they are packed into segments, which happens as they arrive but
     * not for the tables that are never packed. Running out of room part way through is worth a
     * warning beforehand rather than a failure hours later.
     * </p>
     */
    private static void warnAboutFreeSpace(Path source) {
        try {
            long needed = Files.size(source);
            long available = Files.getFileStore(Paths.get(ConfigHandler.path).toAbsolutePath()).getUsableSpace();
            if (available < needed) {
                Chat.console(Color.YELLOW + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_LOW_DISK_SPACE,
                        ColdStorageStats.format(needed), ColdStorageStats.format(available)));
            }
        }
        catch (IOException exception) {
            // The file system could not be asked. The import runs and reports if it does run out.
        }
    }

    /**
     * Works out which of the two databases is damaged and says so.
     *
     * <p>
     * SQLite reports a malformed image against the statement that noticed it, which for a copy is the
     * same statement whichever side the damage is on. Checking both makes the difference between "the
     * file you were importing is broken" and "the file being imported into is broken", which are very
     * different things to be told.
     * </p>
     *
     * @return true when the failure was damage to one of the databases
     */
    private static boolean reportCorruption(Connection connection, Exception exception) {
        if (!isCorruption(exception) || connection == null) {
            return false;
        }

        boolean sourceDamaged = !isIntact(connection, LEGACY);
        boolean targetDamaged = !isIntact(connection, "main");
        if (sourceDamaged) {
            Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_SOURCE_DAMAGED, SOURCE_FILE));
        }
        if (targetDamaged) {
            Chat.console(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.IMPORT_TARGET_DAMAGED, ConfigHandler.sqlite));
        }
        return sourceDamaged || targetDamaged;
    }

    private static boolean isIntact(Connection connection, String schema) {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA " + schema + ".quick_check(1)")) {
            return results.next() && "ok".equalsIgnoreCase(results.getString(1));
        }
        catch (SQLException exception) {
            return false;
        }
    }

    private static Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + ConfigHandler.path + ConfigHandler.sqlite);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=30000");
        }
        return connection;
    }

    private static void attach(Connection connection, Path source) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("ATTACH DATABASE ? AS " + LEGACY)) {
            statement.setString(1, source.toString());
            statement.execute();
        }
    }

    private static void detach(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DETACH DATABASE " + LEGACY);
        }
    }

    /**
     * Renames the imported file rather than deleting it, so it stays recoverable and cannot be
     * imported a second time by accident.
     */
    private static Path archive(Path source) throws SQLException {
        String stamp = ARCHIVE_SUFFIX.format(LocalDateTime.now());
        Path archived = source.resolveSibling(source.getFileName() + ".imported-" + stamp);
        try {
            Files.move(source, archived, StandardCopyOption.ATOMIC_MOVE);
            for (String suffix : new String[] { "-wal", "-shm", "-journal" }) {
                Path sibling = source.resolveSibling(source.getFileName() + suffix);
                if (Files.exists(sibling)) {
                    Files.move(sibling, archived.resolveSibling(archived.getFileName() + suffix), StandardCopyOption.ATOMIC_MOVE);
                }
            }
        }
        catch (IOException exception) {
            throw new SQLException("Unable to set aside the imported database at " + source, exception);
        }
        return archived;
    }

    private static void close(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (SQLException exception) {
            ErrorReporter.report(exception);
        }
    }
}

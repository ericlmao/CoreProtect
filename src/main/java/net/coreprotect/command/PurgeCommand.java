package net.coreprotect.command;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;

import net.coreprotect.bukkit.BukkitAdapter;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.database.PurgeExecutor;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.patch.Patch;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.ChatMessage;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.EntityUtils;
import net.coreprotect.utility.EntitySpawnTracking;
import net.coreprotect.utility.MaterialUtils;
import net.coreprotect.utility.VersionUtils;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.extensions.BackgroundService;

public class PurgeCommand extends Consumer {

    private static final long CONNECTION_DRAIN_TIMEOUT_MILLIS = 60000L;
    private static volatile Thread activePurgeThread;
    private static volatile Statement activePurgeStatement;
    private static volatile boolean shutdownCancellationRequested;

    public static void resetShutdownCancellation() {
        shutdownCancellationRequested = false;
    }

    public static void cancelForShutdown() {
        shutdownCancellationRequested = true;
        Database.cancelClickHousePurge();
        Statement statement = activePurgeStatement;
        if (statement != null) {
            try {
                statement.cancel();
            }
            catch (Exception ignored) {
            }
        }

        Thread thread = activePurgeThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    public static boolean isPurgeWorkerRunning() {
        Thread thread = activePurgeThread;
        return thread != null && thread.isAlive();
    }

    private static void requirePurgeNotCancelled() throws InterruptedException {
        if (shutdownCancellationRequested || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Purge interrupted during shutdown");
        }
    }

    /**
     * Claims the purge lock, asking an automatic purge to stand down first. An automatic purge
     * stops at its next batch and resumes at its next scheduled run, so a manual purge is never
     * blocked for long by one.
     *
     * @return the result of claiming the purge lock
     * @throws InterruptedException
     *             if the purge is cancelled while waiting
     */
    private static Consumer.OperationStartResult claimPurgeAheadOfAutomaticPurge() throws InterruptedException {
        Consumer.OperationStartResult result = Consumer.claimPurge();
        if (result != Consumer.OperationStartResult.PURGE_RUNNING || !Consumer.isBackgroundPurgeRunning()) {
            return result;
        }

        BackgroundService.requestCancel();
        for (int attempt = 0; attempt < 60; attempt++) {
            requirePurgeNotCancelled();
            Thread.sleep(500);
            result = Consumer.claimPurge();
            if (result != Consumer.OperationStartResult.PURGE_RUNNING || !Consumer.isBackgroundPurgeRunning()) {
                return result;
            }
        }

        return result;
    }

    private static PreparedStatement preparePurgeStatement(Connection connection, String query) throws SQLException, InterruptedException {
        requirePurgeNotCancelled();
        PreparedStatement statement = connection.prepareStatement(query);
        activePurgeStatement = statement;
        requirePurgeNotCancelled();
        return statement;
    }

    private static Statement createPurgeStatement(Connection connection) throws SQLException, InterruptedException {
        requirePurgeNotCancelled();
        Statement statement = connection.createStatement();
        activePurgeStatement = statement;
        requirePurgeNotCancelled();
        return statement;
    }

    private static void reportPurgeFailure(Exception exception) throws Exception {
        if (shutdownCancellationRequested || Thread.currentThread().isInterrupted()) {
            throw exception;
        }
        ErrorReporter.report(exception);
    }

    private static String findUnsupportedPurgeArgument(String[] args) {
        boolean includeContinuation = false;
        for (int i = 1; i < args.length; i++) {
            String token = args[i].trim();
            if (token.length() == 0) {
                continue;
            }

            String argument = token.toLowerCase(Locale.ROOT);
            argument = argument.replaceAll("\\\\", "");
            argument = argument.replaceAll("'", "");

            if (includeContinuation) {
                includeContinuation = argument.endsWith(",");
                continue;
            }

            if (argument.equals("#optimize")) {
                continue;
            }

            if (argument.startsWith("i:") || argument.startsWith("include:") || argument.startsWith("item:") || argument.startsWith("items:") || argument.startsWith("b:") || argument.startsWith("block:") || argument.startsWith("blocks:")) {
                String includeValues = argument.replaceAll("include:", "").replaceAll("i:", "").replaceAll("items:", "").replaceAll("item:", "").replaceAll("blocks:", "").replaceAll("block:", "").replaceAll("b:", "");
                includeContinuation = includeValues.length() == 0 || includeValues.endsWith(",");
                continue;
            }

            if (argument.startsWith("t:") || argument.startsWith("time:")) {
                continue;
            }

            if (argument.startsWith("r:") || argument.startsWith("radius:")) {
                continue;
            }

            if (argument.contains(":")) {
                return token;
            }
        }

        return null;
    }

    protected static void runCommand(final CommandSender player, boolean permission, String[] args) {
        int resultc = args.length;
        Location location = CommandParser.parseLocation(player, args);
        final Integer[] argRadius = CommandParser.parseRadius(args, player, location);
        final List<Integer> argAction = CommandParser.parseAction(args);
        final List<Object> argBlocks = CommandParser.parseRestricted(player, args, argAction);
        final Map<Object, Boolean> argExclude = CommandParser.parseExcluded(player, args, argAction);
        final List<String> argExcludeUsers = CommandParser.parseExcludedUsers(player, args);
        final long[] argTime = CommandParser.parseTime(args);
        final int argWid = CommandParser.parseWorld(args, false, false);
        final List<Integer> supportedActions = Arrays.asList();
        long startTime = argTime[1] > 0 ? argTime[0] : 0;
        long endTime = argTime[1] > 0 ? argTime[1] : argTime[0];

        if (argBlocks == null || argExclude == null || argExcludeUsers == null) {
            return;
        }

        if (ConfigHandler.converterRunning || ConfigHandler.migrationRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.UPGRADE_IN_PROGRESS));
            return;
        }
        if (ConfigHandler.purgeRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_IN_PROGRESS));
            return;
        }
        if (!permission) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
            return;
        }
        if (resultc <= 1) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MISSING_PARAMETERS, "/co purge t:<time>"));
            return;
        }
        String unsupportedArgument = findUnsupportedPurgeArgument(args);
        if (unsupportedArgument != null) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.INVALID_PARAMETER, unsupportedArgument));
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MISSING_PARAMETERS, "/co help purge"));
            return;
        }
        if (endTime <= 0) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MISSING_PARAMETERS, "/co purge t:<time>"));
            return;
        }
        if (argRadius != null) {
            Chat.sendMessage(player, new ChatMessage(Phrase.build(Phrase.INVALID_WORLD)).build());
            return;
        }
        if (argWid == -1) {
            String worldName = CommandParser.parseWorldName(args, false);
            Chat.sendMessage(player, new ChatMessage(Phrase.build(Phrase.WORLD_NOT_FOUND, worldName)).build());
            return;
        }
        if (player instanceof Player && endTime < 2592000) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_MINIMUM_TIME, "30", Selector.FIRST)); // 30 days
            return;
        }
        else if (endTime < 86400) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_MINIMUM_TIME, "24", Selector.SECOND)); // 24 hours
            return;
        }
        for (int action : argAction) {
            if (!supportedActions.contains(action)) {
                Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.ACTION_NOT_SUPPORTED));
                // Functions.sendMessage(player, new ChatMessage("Please specify a valid purge action.").build());
                return;
            }
        }

        StringBuilder restrict = new StringBuilder();
        String includeBlock = "";
        List<Integer> includeBlockIds = new ArrayList<>();
        String includeEntity = "";
        boolean hasBlock = false;
        boolean item = false;
        boolean entity = false;
        int restrictCount = 0;

        if (argBlocks.size() > 0) {
            StringBuilder includeListMaterial = new StringBuilder();
            StringBuilder includeListEntity = new StringBuilder();

            for (Object restrictTarget : argBlocks) {
                String targetName = "";

                if (restrictTarget instanceof Material) {
                    targetName = ((Material) restrictTarget).name();
                    int blockId = MaterialUtils.getBlockId(targetName, false);
                    includeBlockIds.add(blockId);
                    if (includeListMaterial.length() == 0) {
                        includeListMaterial = includeListMaterial.append(blockId);
                    }
                    else {
                        includeListMaterial.append(",").append(blockId);
                    }

                    /* Include legacy IDs */
                    int legacyId = BukkitAdapter.ADAPTER.getLegacyBlockId((Material) restrictTarget);
                    if (legacyId > 0) {
                        includeListMaterial.append(",").append(legacyId);
                        includeBlockIds.add(legacyId);
                    }

                    targetName = ((Material) restrictTarget).name().toLowerCase(Locale.ROOT);
                    item = (!item ? !(((Material) restrictTarget).isBlock()) : item);
                    hasBlock = true;
                }
                else if (restrictTarget instanceof EntityType) {
                    targetName = ((EntityType) restrictTarget).name();
                    if (includeListEntity.length() == 0) {
                        includeListEntity = includeListEntity.append(EntityUtils.getEntityId(targetName, false));
                    }
                    else {
                        includeListEntity.append(",").append(EntityUtils.getEntityId(targetName, false));
                    }

                    targetName = ((EntityType) restrictTarget).name().toLowerCase(Locale.ROOT);
                    entity = true;
                }
                else if (restrictTarget instanceof String) {
                    int blockId = MaterialUtils.getBlockId((String) restrictTarget, false);
                    includeBlockIds.add(blockId);
                    if (includeListMaterial.length() == 0) {
                        includeListMaterial = includeListMaterial.append(blockId);
                    }
                    else {
                        includeListMaterial.append(",").append(blockId);
                    }

                    targetName = ((String) restrictTarget).toLowerCase(Locale.ROOT);
                    hasBlock = true;
                }

                if (restrictCount == 0) {
                    restrict = restrict.append("" + targetName + "");
                }
                else {
                    restrict.append(", ").append(targetName);
                }

                restrictCount++;
            }

            includeBlock = includeListMaterial.toString();
            includeEntity = includeListEntity.toString();
        }

        if (entity) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.ACTION_NOT_SUPPORTED));
            return;
        }

        boolean optimizeCheck = false;
        for (String arg : args) {
            if (arg.trim().equalsIgnoreCase("#optimize")) {
                optimizeCheck = true;
                break;
            }
        }

        final StringBuilder restrictTargets = restrict;
        final String includeBlockFinal = includeBlock;
        final List<Integer> includeBlockIdsFinal = List.copyOf(includeBlockIds);
        final boolean optimize = optimizeCheck;
        final boolean hasBlockRestriction = hasBlock;
        final int restrictCountFinal = restrictCount;

        class BasicThread implements Runnable {

            @Override
            public void run() {
                boolean purgeClaimed = false;
                boolean consumerPaused = false;
                boolean duckTransaction = false;
                boolean duckPurgeStarted = false;
                AtomicBoolean duckCommitAttempted = new AtomicBoolean();
                boolean duckRollbackSucceeded = false;
                boolean maintenanceLocked = false;
                boolean resumePersistence = true;
                boolean handoffStarted = false;
                Connection connection = null;
                Statement transactionStatement = null;
                try {
                    requirePurgeNotCancelled();
                    if (Consumer.isPersistenceHalted()) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                        return;
                    }
                    long timestamp = (System.currentTimeMillis() / 1000L);
                    long timeStart = startTime > 0 ? (timestamp - startTime) : 0;
                    long timeEnd = timestamp - endTime;
                    long removed = 0;

                    for (int i = 0; i <= 5; i++) {
                        requirePurgeNotCancelled();
                        connection = Database.getConnection(false, 500);
                        if (connection != null) {
                            break;
                        }
                        Thread.sleep(1000);
                    }

                    if (connection == null) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                        return;
                    }

                    requirePurgeNotCancelled();
                    Consumer.OperationStartResult startResult = claimPurgeAheadOfAutomaticPurge();
                    if (startResult != Consumer.OperationStartResult.STARTED) {
                        Phrase phrase = startResult == Consumer.OperationStartResult.PURGE_RUNNING ? Phrase.PURGE_IN_PROGRESS
                                : startResult == Consumer.OperationStartResult.RELOAD_RUNNING ? Phrase.DATABASE_BUSY
                                        : startResult == Consumer.OperationStartResult.PERSISTENCE_HALTED ? Phrase.DATABASE_PERSISTENCE_HALTED : Phrase.ROLLBACK_IN_PROGRESS;
                        Chat.sendGlobalMessage(player, Phrase.build(phrase));
                        try {
                            connection.close();
                        }
                        catch (Exception e) {
                            ErrorReporter.report(e);
                        }
                        return;
                    }
                    purgeClaimed = true;
                    activePurgeThread = Thread.currentThread();
                    requirePurgeNotCancelled();

                    if (argWid > 0) {
                        String worldName = CommandParser.parseWorldName(args, false);
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_STARTED, worldName));
                    }
                    else {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_STARTED, "#global"));
                    }

                    if (hasBlockRestriction) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.ROLLBACK_INCLUDE, restrictTargets.toString(), Selector.FIRST, Selector.FIRST, (restrictCountFinal == 1 ? Selector.FIRST : Selector.SECOND))); // include
                    }

                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_NOTICE_1));
                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_NOTICE_2));

                    while (!Consumer.pausedSuccess && !Consumer.isPersistenceHalted()) {
                        Thread.sleep(1);
                    }
                    if (Consumer.isPersistenceHalted()) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                        return;
                    }
                    Consumer.isPaused = true;
                    consumerPaused = true;
                    requirePurgeNotCancelled();

                    if (ConfigHandler.databaseType.isClickHouse()) {
                        connection.close();
                        connection = null;
                        if (optimize) {
                            Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_OPTIMIZING));
                        }
                        requirePurgeNotCancelled();
                        removed = Database.purgeClickHouse(timeStart, timeEnd, argWid, includeBlockIdsFinal, optimize);
                        EntitySpawnTracking.invalidateDatabaseVerification();
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_SUCCESS));
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_ROWS, NumberFormat.getInstance().format(removed), (removed == 1 ? Selector.FIRST : Selector.SECOND)));
                        return;
                    }

                    String query = "";
                    PreparedStatement preparedStmt = null;

                    Integer[] lastVersion = Patch.getDatabaseVersion(connection, true);
                    boolean newVersion = VersionUtils.newVersion(lastVersion, VersionUtils.getInternalPluginVersion());
                    if (newVersion && !ConfigHandler.EDITION_BRANCH.contains("-dev")) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_FAILED));
                        return;
                    }

                    if (ConfigHandler.databaseType.isDuckDB()) {
                        transactionStatement = createPurgeStatement(connection);
                        Database.beginTransaction(transactionStatement, ConfigHandler.databaseType);
                        duckPurgeStarted = true;
                        duckTransaction = true;
                    }

                    PurgeExecutor.StatementFactory statementFactory = PurgeCommand::preparePurgeStatement;
                    PurgeExecutor.BatchCallback batchCallback = PurgeCommand::requirePurgeNotCancelled;

                    for (String table : ConfigHandler.databaseTables) {
                        requirePurgeNotCancelled();
                        String tableName = table.replaceAll("_", " ");
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_PROCESSING, tableName));

                        try {
                            String blockRestriction = null;
                            if (hasBlockRestriction) {
                                blockRestriction = "action NOT IN(" + LookupActions.ENTITY_KILL + "," + LookupActions.ENTITY_SPAWN + ") AND type IN(" + includeBlockFinal + ") AND ";
                            }

                            removed = removed + PurgeExecutor.purgeTable(connection, table, timeStart, timeEnd, argWid, blockRestriction, PurgeExecutor.DEFAULT_BATCH_SIZE, statementFactory, batchCallback);
                        }
                        catch (Exception e) {
                            if (ConfigHandler.databaseType.isDuckDB()) {
                                throw e;
                            }
                            if (!ConfigHandler.serverRunning) {
                                Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_FAILED));
                                return;
                            }

                            reportPurgeFailure(e);
                        }
                    }

                    requirePurgeNotCancelled();
                    if (ConfigHandler.databaseType.isSQLite() && argWid == 0 && !hasBlockRestriction) {
                        // Compressed storage is dropped whole segments at a time. A purge limited to
                        // one world or to certain blocks cannot do that without rewriting segments,
                        // so those purges only cover the data that is still in the live tables.
                        removed = removed + PurgeExecutor.purgeColdSegments(connection, timeEnd, statementFactory, batchCallback);
                    }

                    removed = removed + PurgeExecutor.removeOrphanedRows(connection, statementFactory, batchCallback);

                    if (ConfigHandler.databaseType.isMySQL() && optimize) {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_OPTIMIZING));
                        for (String table : ConfigHandler.databaseTables) {
                            requirePurgeNotCancelled();
                            query = "OPTIMIZE LOCAL TABLE " + ConfigHandler.prefix + table + "";
                            preparedStmt = preparePurgeStatement(connection, query);
                            preparedStmt.execute();
                            preparedStmt.close();
                        }
                    }

                    if (duckTransaction) {
                        activePurgeStatement = transactionStatement;
                        requirePurgeNotCancelled();
                        if (!Database.commitTransactionChecked(transactionStatement, ConfigHandler.databaseType, () -> duckCommitAttempted.set(true))) {
                            throw new SQLException("Unable to commit DuckDB purge transaction");
                        }
                        duckTransaction = false;
                        duckPurgeStarted = false;
                        duckCommitAttempted.set(false);
                        requirePurgeNotCancelled();
                        try {
                            transactionStatement.execute("CHECKPOINT");
                        }
                        catch (SQLException e) {
                            if (!shutdownCancellationRequested) {
                                ErrorReporter.report(e);
                            }
                        }
                        transactionStatement.close();
                        transactionStatement = null;
                    }

                    if (ConfigHandler.databaseType.isSQLite()) {
                        // Deleted pages are reused by future writes; truncating the write-ahead log
                        // releases the space it grew to while the rows were being removed.
                        try (Statement checkpointStatement = createPurgeStatement(connection)) {
                            Database.performCheckpoint(checkpointStatement, ConfigHandler.databaseType);
                        }
                    }

                    connection.close();
                    connection = null;
                    requirePurgeNotCancelled();

                    try {
                        Consumer.lockDatabaseMaintenanceInterruptibly();
                        maintenanceLocked = true;
                        if (!Database.awaitConnectionDrain(CONNECTION_DRAIN_TIMEOUT_MILLIS)) {
                            throw new SQLException("Timed out waiting for active database connections before purge handoff");
                        }
                        requirePurgeNotCancelled();
                    }
                    catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw exception;
                    }

                    resumePersistence = false;
                    handoffStarted = true;
                    ConfigHandler.loadDatabase();
                    handoffStarted = false;
                    resumePersistence = true;
                    EntitySpawnTracking.invalidateDatabaseVerification();

                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_SUCCESS));
                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_ROWS, NumberFormat.getInstance().format(removed), (removed == 1 ? Selector.FIRST : Selector.SECOND)));
                }
                catch (Exception e) {
                    boolean shutdownCancelled = shutdownCancellationRequested || e instanceof InterruptedException;
                    boolean reloadRequired = false;
                    if (duckTransaction && transactionStatement != null) {
                        duckRollbackSucceeded = Database.rollbackTransaction(transactionStatement, ConfigHandler.databaseType);
                    }
                    if (ConfigHandler.databaseType.isDuckDB() && requiresDuckDatabaseReload(duckPurgeStarted, duckCommitAttempted.get(), duckRollbackSucceeded)) {
                        Consumer.requireDatabaseReload();
                        ConfigHandler.databaseReachable = false;
                        resumePersistence = false;
                        reloadRequired = true;
                    }
                    if (handoffStarted) {
                        Consumer.requireDatabaseReload();
                        ConfigHandler.databaseReachable = false;
                        reloadRequired = true;
                    }
                    if (shutdownCancelled) {
                        Thread.currentThread().interrupt();
                    }
                    else {
                        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_FAILED));
                        if (reloadRequired) {
                            Chat.sendGlobalMessage(player, Phrase.build(Phrase.RELOAD_FAILED));
                        }
                        ErrorReporter.report(e);
                    }
                }
                finally {
                    if (transactionStatement != null) {
                        try {
                            transactionStatement.close();
                        }
                        catch (Exception e) {
                            ErrorReporter.report(e);
                        }
                    }
                    if (connection != null) {
                        try {
                            connection.close();
                        }
                        catch (Exception e) {
                            ErrorReporter.report(e);
                        }
                    }
                    if (maintenanceLocked) {
                        Consumer.unlockDatabaseMaintenance();
                    }
                    if (consumerPaused && resumePersistence && !Consumer.isPersistenceHalted()) {
                        Consumer.isPaused = false;
                    }
                    if (purgeClaimed) {
                        Consumer.releasePurge();
                    }
                    if (activePurgeThread == Thread.currentThread()) {
                        activePurgeStatement = null;
                        activePurgeThread = null;
                    }
                }
            }
        }

        Runnable runnable = new BasicThread();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    static boolean requiresDuckDatabaseReload(boolean purgeStarted, boolean commitAttempted, boolean rollbackSucceeded) {
        return purgeStarted && (commitAttempted || !rollbackSucceeded);
    }
}

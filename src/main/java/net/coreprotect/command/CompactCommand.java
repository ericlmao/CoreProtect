package net.coreprotect.command;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;

import org.bukkit.command.CommandSender;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.BlobRecompressTask;
import net.coreprotect.database.ColdRollupTask;
import net.coreprotect.database.ColdStorageStats;
import net.coreprotect.database.CompactProgress;
import net.coreprotect.database.Database;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.ErrorReporter;

/**
 * Packs activity that has aged out of the hot window into compressed storage on demand, instead of
 * waiting for the nightly run.
 *
 * <p>
 * The work is the same as the automatic roll-up: rows are sealed into compressed segments oldest
 * first and removed from the live tables in one transaction per segment, so the database is never
 * duplicated and an interrupted run simply leaves the remaining rows where they are.
 * </p>
 */
public class CompactCommand {

    private CompactCommand() {
        throw new IllegalStateException("Command class");
    }

    protected static void runCommand(final CommandSender player, boolean permission, String[] args) {
        if (!permission) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
            return;
        }
        if (!ConfigHandler.databaseType.isSQLite()) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.COMPACT_UNSUPPORTED, ConfigHandler.databaseType.getDisplayName()));
            return;
        }
        if (ConfigHandler.converterRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.UPGRADE_IN_PROGRESS));
            return;
        }
        if (ConfigHandler.purgeRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_IN_PROGRESS));
            return;
        }

        Thread worker = new Thread(() -> compact(player), "CoreProtect-Compact");
        worker.start();
    }

    /** Seconds between saying how far along the compact is. */
    private static final long PROGRESS_INTERVAL = 15;

    /** When the next progress line is due, so a long run says something without saying it constantly. */
    private static long nextProgress;

    private static void compact(CommandSender player) {
        Consumer.OperationStartResult startResult = Consumer.claimBackgroundPurge();
        if (startResult != Consumer.OperationStartResult.STARTED) {
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
            return;
        }

        Connection connection = null;
        try {
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_STARTED));
            nextProgress = (System.currentTimeMillis() / 1000L) + PROGRESS_INTERVAL;

            for (int attempt = 0; attempt < 6 && connection == null; attempt++) {
                connection = Database.getConnection(false, 500);
                if (connection == null) {
                    Thread.sleep(1000);
                }
            }

            if (connection == null) {
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                return;
            }

            // A manual compact packs everything that has already been written, rather than waiting
            // for the hot window to expire.
            // Anything an earlier build left numbered underneath the segments is put back above them
            // first, or it can never be sealed.
            ColdRollupTask.repairRowIds(connection, () -> tick(player));

            long sealBefore = (System.currentTimeMillis() / 1000L) + 1;
            long sealed = ColdRollupTask.rollUp(connection, () -> tick(player), sealBefore);
            // Segments written by older builds have no per player counts, which lookups rely on to
            // avoid opening segments that hold nothing for the player being searched for.
            ColdRollupTask.backfillStatistics(connection, () -> tick(player));
            // Entity data is read one row at a time and so never reaches a segment. It is compressed
            // where it lies instead, against a dictionary that supplies the repetition a single blob
            // of a couple of kilobytes does not contain.
            long blobBytes = BlobRecompressTask.run(connection, () -> tick(player));
            reclaimSpace(connection, player);

            CompactProgress.clear();
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_COMPLETED, NumberFormat.getInstance().format(sealed), (sealed == 1 ? Selector.FIRST : Selector.SECOND)));
            if (blobBytes > 0) {
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_BLOBS, ColdStorageStats.format(blobBytes)));
            }
            if (sealed == 0 && blobBytes == 0) {
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_NOTHING_TO_DO));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_STOPPED));
        }
        catch (Exception e) {
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_FAILED));
            ErrorReporter.report(e);
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception e) {
                    ErrorReporter.report(e);
                }
            }
            Consumer.releaseBackgroundPurge();
        }
    }

    /**
     * Hands the pages freed by the roll-up back to the file system. Compressed segments are a
     * fraction of the size of the rows they replace, so most of the file becomes free space.
     *
     * @param connection
     *            an open connection
     * @param player
     *            whoever asked for the compact, told how it is going
     */
    private static void reclaimSpace(Connection connection, CommandSender player) {
        try {
            Database.reclaimFreePages(connection, () -> tick(player));
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
        }
        catch (SQLException e) {
            // The space is reused by future writes even when it cannot be returned right now.
            ErrorReporter.report(e);
        }
    }

    /**
     * Stops the run when the database is needed for something else. Sealed segments are kept, and
     * the rest is packed by the next run.
     *
     * @throws InterruptedException
     *             if the run should stop
     */
    /**
     * Called between pieces of work: stops the run when the database is needed elsewhere, and says
     * how far along it is often enough to show it is moving without filling the console.
     *
     * @param player
     *            whoever asked for the compact
     * @throws InterruptedException
     *             if the run should stop
     */
    private static void tick(CommandSender player) throws InterruptedException {
        checkCancelled();

        long now = System.currentTimeMillis() / 1000L;
        if (now < nextProgress) {
            return;
        }
        nextProgress = now + PROGRESS_INTERVAL;

        String progress = CompactProgress.line();
        if (progress != null) {
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.COMPACT_PROGRESS, progress));
        }
    }

    private static void checkCancelled() throws InterruptedException {
        if (!ConfigHandler.serverRunning || ConfigHandler.purgeRunning || ConfigHandler.converterRunning || ConfigHandler.migrationRunning || Consumer.isPersistenceHalted()) {
            throw new InterruptedException("Compacting stopped");
        }
    }
}

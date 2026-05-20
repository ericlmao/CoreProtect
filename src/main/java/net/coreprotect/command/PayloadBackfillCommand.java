package net.coreprotect.command;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.bukkit.command.CommandSender;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.payload.PayloadStorage;
import net.coreprotect.language.Phrase;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;

public class PayloadBackfillCommand {

    private static final List<Target> TARGETS = Arrays.asList(
            new Target("block", "meta", "meta_payload_id"),
            new Target("block", "blockdata", "blockdata_payload_id"),
            new Target("container", "metadata", "metadata_payload_id"),
            new Target("item", "data", "data_payload_id"),
            new Target("entity", "data", "data_payload_id")
    );

    private PayloadBackfillCommand() {
        throw new IllegalStateException("Command class");
    }

    protected static void runCommand(CommandSender sender, boolean permission, String[] args) {
        if (!permission) {
            Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
            return;
        }

        if (Config.getGlobal().MYSQL) {
            Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Payload backfill is only available for SQLite.");
            return;
        }

        boolean dryRun = false;
        int batchSize = 1000;
        for (String arg : args) {
            String normalized = arg.toLowerCase(Locale.ROOT);
            if (normalized.equals("dry-run") || normalized.equals("#dry-run")) {
                dryRun = true;
            }
            else if (normalized.startsWith("batch:")) {
                try {
                    batchSize = Math.max(1, Math.min(10000, Integer.parseInt(normalized.substring("batch:".length()))));
                }
                catch (Exception e) {
                    Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Invalid batch size.");
                    return;
                }
            }
        }

        final boolean finalDryRun = dryRun;
        final int finalBatchSize = batchSize;
        Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Payload backfill starting. Make sure you have a database backup.");
        Thread thread = new Thread(() -> runBackfill(sender, finalDryRun, finalBatchSize), "CoreProtect-PayloadBackfill");
        thread.start();
    }

    private static void runBackfill(CommandSender sender, boolean dryRun, int batchSize) {
        long scanned = 0;
        long updated = 0;
        try (Connection connection = Database.getConnection(false, 1000)) {
            if (connection == null) {
                Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.DATABASE_BUSY));
                return;
            }

            for (Target target : TARGETS) {
                while (true) {
                    int processed = processBatch(connection, target, dryRun, batchSize);
                    scanned += processed;
                    if (!dryRun) {
                        updated += processed;
                    }
                    if (dryRun) {
                        break;
                    }
                    if (processed < batchSize) {
                        break;
                    }
                    Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Payload backfill progress: " + scanned + " rows scanned.");
                    Thread.sleep(25L);
                }
            }

            Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Payload backfill complete. Rows scanned: " + scanned + ", rows updated: " + updated + ".");
        }
        catch (Exception e) {
            Chat.sendMessage(sender, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- Payload backfill failed. Check console for details.");
            e.printStackTrace();
        }
    }

    private static int processBatch(Connection connection, Target target, boolean dryRun, int batchSize) throws Exception {
        String table = ConfigHandler.prefix + target.table;
        if (dryRun) {
            String countQuery = "SELECT COUNT(*) AS count FROM " + table + " WHERE " + target.inlineColumn + " IS NOT NULL AND (" + target.payloadIdColumn + " IS NULL OR " + target.payloadIdColumn + " = 0)";
            try (PreparedStatement count = connection.prepareStatement(countQuery);
                 ResultSet resultSet = count.executeQuery()) {
                return resultSet.next() ? resultSet.getInt("count") : 0;
            }
        }

        String query = "SELECT rowid," + target.inlineColumn + " FROM " + table + " WHERE " + target.inlineColumn + " IS NOT NULL AND (" + target.payloadIdColumn + " IS NULL OR " + target.payloadIdColumn + " = 0) LIMIT ?";
        String update = "UPDATE " + table + " SET " + target.payloadIdColumn + " = ?" + (Config.getGlobal().SQLITE_PAYLOAD_KEEP_LEGACY_INLINE_VALUES ? "" : ", " + target.inlineColumn + " = NULL") + " WHERE rowid = ?";
        int processed = 0;

        try (PreparedStatement select = connection.prepareStatement(query);
             PreparedStatement updateStatement = connection.prepareStatement(update)) {
            select.setInt(1, batchSize);
            try (ResultSet resultSet = select.executeQuery()) {
                while (resultSet.next()) {
                    processed++;
                    if (dryRun) {
                        continue;
                    }

                    long rowId = resultSet.getLong("rowid");
                    byte[] raw = resultSet.getBytes(target.inlineColumn);
                    long payloadId = PayloadStorage.store(connection, raw);
                    if (payloadId <= 0) {
                        continue;
                    }
                    updateStatement.setLong(1, payloadId);
                    updateStatement.setLong(2, rowId);
                    updateStatement.addBatch();
                }
            }

            if (!dryRun) {
                updateStatement.executeBatch();
            }
        }

        return processed;
    }

    private static final class Target {
        private final String table;
        private final String inlineColumn;
        private final String payloadIdColumn;

        private Target(String table, String inlineColumn, String payloadIdColumn) {
            this.table = table;
            this.inlineColumn = inlineColumn;
            this.payloadIdColumn = payloadIdColumn;
        }
    }
}

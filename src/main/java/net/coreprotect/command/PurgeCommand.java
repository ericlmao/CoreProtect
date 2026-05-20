package net.coreprotect.command;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;

import net.coreprotect.bukkit.BukkitAdapter;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.patch.Patch;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.ChatMessage;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.EntityUtils;
import net.coreprotect.utility.MaterialUtils;
import net.coreprotect.utility.VersionUtils;

public class PurgeCommand extends Consumer {

    public static final List<String> PURGE_TABLES = Arrays.asList("sign", "container", "item", "skull", "session", "chat", "command", "entity", "block");

    private static class PurgeRequest {
        private final CommandSender sender;
        private final String[] args;
        private final long startTime;
        private final long endTime;
        private final int worldId;
        private final String restrictTargets;
        private final String includeBlock;
        private final boolean optimize;
        private final boolean hasBlockRestriction;
        private final int restrictCount;
        private final boolean autoPurge;

        private PurgeRequest(CommandSender sender, String[] args, long startTime, long endTime, int worldId, String restrictTargets, String includeBlock, boolean optimize, boolean hasBlockRestriction, int restrictCount, boolean autoPurge) {
            this.sender = sender;
            this.args = args;
            this.startTime = startTime;
            this.endTime = endTime;
            this.worldId = worldId;
            this.restrictTargets = restrictTargets;
            this.includeBlock = includeBlock;
            this.optimize = optimize;
            this.hasBlockRestriction = hasBlockRestriction;
            this.restrictCount = restrictCount;
            this.autoPurge = autoPurge;
        }
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

    public static void runAutoPurge() {
        String autoPurge = Config.getGlobal().AUTO_PURGE;
        if (autoPurge == null) {
            return;
        }

        autoPurge = autoPurge.trim();
        if (autoPurge.length() == 0 || autoPurge.equalsIgnoreCase("false")) {
            return;
        }
        if (ConfigHandler.converterRunning || ConfigHandler.migrationRunning || ConfigHandler.purgeRunning) {
            return;
        }

        String[] args = new String[] { "purge", "t:" + autoPurge };
        long[] argTime = CommandParser.parseTime(args);
        long startTime = argTime[1] > 0 ? argTime[0] : 0;
        long endTime = argTime[1] > 0 ? argTime[1] : argTime[0];
        if (endTime <= 0) {
            return;
        }

        PurgeRequest request = new PurgeRequest(Bukkit.getConsoleSender(), args, startTime, endTime, 0, "", "", false, false, 0, true);
        startPurgeThread(request);
    }

    private static void startPurgeThread(final PurgeRequest request) {
        class BasicThread implements Runnable {

            @Override
            public void run() {
                try {
                    long timestamp = (System.currentTimeMillis() / 1000L);
                    long timeStart = request.startTime > 0 ? (timestamp - request.startTime) : 0;
                    long timeEnd = timestamp - request.endTime;
                    long removed = 0;

                    Connection connection = null;
                    for (int i = 0; i <= 5; i++) {
                        connection = Database.getConnection(false, 500);
                        if (connection != null) {
                            break;
                        }
                        Thread.sleep(1000);
                    }

                    if (connection == null) {
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.DATABASE_BUSY));
                        return;
                    }

                    if (request.worldId > 0) {
                        String worldName = CommandParser.parseWorldName(request.args, false);
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_STARTED, worldName));
                    }
                    else {
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_STARTED, "#global"));
                    }

                    if (request.hasBlockRestriction) {
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.ROLLBACK_INCLUDE, request.restrictTargets, Selector.FIRST, Selector.FIRST, (request.restrictCount == 1 ? Selector.FIRST : Selector.SECOND))); // include
                    }

                    Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_NOTICE_1));
                    Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_NOTICE_2));

                    ConfigHandler.purgeRunning = true;
                    while (!Consumer.pausedSuccess) {
                        Thread.sleep(1);
                    }
                    Consumer.isPaused = true;

                    String query = "";
                    PreparedStatement preparedStmt = null;

                    Integer[] lastVersion = Patch.getDatabaseVersion(connection, true);
                    boolean newVersion = VersionUtils.newVersion(lastVersion, VersionUtils.getInternalPluginVersion());
                    if (newVersion && !ConfigHandler.EDITION_BRANCH.contains("-dev")) {
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_FAILED));
                        Consumer.isPaused = false;
                        ConfigHandler.purgeRunning = false;
                        return;
                    }

                    List<String> worldTables = Arrays.asList("sign", "container", "item", "session", "chat", "command", "block");
                    List<String> restrictTables = Arrays.asList("block");
                    for (String table : ConfigHandler.databaseTables) {
                        String tableName = table.replaceAll("_", " ");
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_PROCESSING, tableName));

                        try {
                            boolean purge = PURGE_TABLES.contains(table);

                            String blockRestriction = "";
                            if (request.hasBlockRestriction && restrictTables.contains(table)) {
                                blockRestriction = "type IN(" + request.includeBlock + ") AND ";
                            }
                            else if (request.hasBlockRestriction) {
                                purge = false;
                            }

                            String worldRestriction = "";
                            if (request.worldId > 0 && worldTables.contains(table)) {
                                worldRestriction = " AND wid = '" + request.worldId + "'";
                            }
                            else if (request.worldId > 0) {
                                purge = false;
                            }

                            if (purge) {
                                query = "DELETE FROM " + ConfigHandler.prefix + table + " WHERE " + blockRestriction + "time < '" + timeEnd + "' AND time >= '" + timeStart + "'" + worldRestriction;
                                preparedStmt = connection.prepareStatement(query);
                                preparedStmt.execute();
                                removed = removed + preparedStmt.getUpdateCount();
                                preparedStmt.close();
                            }
                        }
                        catch (Exception e) {
                            if (!ConfigHandler.serverRunning) {
                                Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_FAILED));
                                return;
                            }

                            e.printStackTrace();
                        }
                    }

                    if (Config.getGlobal().MYSQL && request.optimize) {
                        Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_OPTIMIZING));
                        for (String table : ConfigHandler.databaseTables) {
                            query = "OPTIMIZE LOCAL TABLE " + ConfigHandler.prefix + table + "";
                            preparedStmt = connection.prepareStatement(query);
                            preparedStmt.execute();
                            preparedStmt.close();
                        }
                    }

                    connection.close();
                    ConfigHandler.loadDatabase();
                    if (request.autoPurge) {
                        ConfigHandler.autoPurgeRowsPurged.addAndGet(removed);
                    }

                    Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_SUCCESS));
                    Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_ROWS, NumberFormat.getInstance().format(removed), (removed == 1 ? Selector.FIRST : Selector.SECOND)));
                }
                catch (Exception e) {
                    Chat.sendGlobalMessage(request.sender, Phrase.build(Phrase.PURGE_FAILED));
                    e.printStackTrace();
                }

                Consumer.isPaused = false;
                ConfigHandler.purgeRunning = false;
            }
        }

        Runnable runnable = new BasicThread();
        Thread thread = new Thread(runnable);
        thread.start();
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
                    if (includeListMaterial.length() == 0) {
                        includeListMaterial = includeListMaterial.append(MaterialUtils.getBlockId(targetName, false));
                    }
                    else {
                        includeListMaterial.append(",").append(MaterialUtils.getBlockId(targetName, false));
                    }

                    /* Include legacy IDs */
                    int legacyId = BukkitAdapter.ADAPTER.getLegacyBlockId((Material) restrictTarget);
                    if (legacyId > 0) {
                        includeListMaterial.append(",").append(legacyId);
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

        PurgeRequest request = new PurgeRequest(player, args, startTime, endTime, argWid, restrict.toString(), includeBlock, optimizeCheck, hasBlock, restrictCount, false);
        startPurgeThread(request);
    }
}

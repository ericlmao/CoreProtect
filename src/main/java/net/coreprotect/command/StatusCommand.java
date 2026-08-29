package net.coreprotect.command;

import java.util.concurrent.ConcurrentHashMap;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.PluginDescriptionFile;

import java.sql.Connection;

import net.coreprotect.CoreProtect;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.consumer.process.Process;
import net.coreprotect.database.ColdStorageStats;
import net.coreprotect.database.LegacyImport;
import net.coreprotect.database.Database;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.patch.Patch;
import net.coreprotect.thread.NetworkHandler;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.SystemUtils;
import net.coreprotect.utility.VersionUtils;
import net.coreprotect.utility.ErrorReporter;

public class StatusCommand {
    private static ConcurrentHashMap<String, Boolean> alert = new ConcurrentHashMap<>();

    protected static void runCommand(CommandSender player, boolean permission, String[] args) {
        if (!permission) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
            return;
        }

        class BasicThread implements Runnable {
            @Override
            public void run() {
                try {
                    CoreProtect instance = CoreProtect.getInstance();
                    PluginDescriptionFile pdfFile = instance.getDescription();

                    Chat.sendMessage(player, Color.WHITE + "----- " + Color.DARK_AQUA + "CoreProtect" + Color.WHITE + " -----");
                    Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_VERSION, Color.WHITE, ConfigHandler.EDITION_NAME + " v" + pdfFile.getVersion() + "."));

                    /*
                        Items processed (since server start)
                        Items processed (last 60 minutes)
                     */

                    // Using MySQL/SQLite (Database Size: 587MB)

                    String firstVersion = ConfigHandler.databaseType.isClickHouse() ? "" : Patch.getFirstVersion();
                    if (firstVersion.length() > 0) {
                        firstVersion = " (" + Phrase.build(Phrase.FIRST_VERSION, firstVersion) + ")";
                    }
                    String databaseName = ConfigHandler.databaseType.getDisplayName();
                    if (Consumer.isPersistenceHalted()) {
                        databaseName += " " + Color.RED + Phrase.build(Phrase.STATUS_DATABASE_STATE, Selector.FIRST) + Color.WHITE;
                    }
                    else if (!ConfigHandler.databaseReachable) {
                        databaseName += " " + Color.RED + Phrase.build(Phrase.STATUS_DATABASE_STATE, Selector.SECOND) + Color.WHITE;
                    }
                    Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_DATABASE, Color.WHITE, databaseName) + firstVersion);

                    sendStorageBreakdown(player);

                    if (ConfigHandler.worldeditEnabled) {
                        Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_INTEGRATION, Color.WHITE, "WorldEdit", Selector.FIRST));
                    }
                    else if (instance.getServer().getPluginManager().getPlugin("WorldEdit") != null) {
                        Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_INTEGRATION, Color.WHITE, "WorldEdit", Selector.SECOND));
                    }

                    try {
                        int consumerCount = 0;
                        int currentConsumerSize = Process.getCurrentConsumerSize();
                        if (currentConsumerSize == 0) {
                            consumerCount = Consumer.getConsumerSize(0) + Consumer.getConsumerSize(1);
                        }
                        else {
                            int consumerId = (Consumer.currentConsumer == 1) ? 1 : 0;
                            consumerCount = Consumer.getConsumerSize(consumerId) + currentConsumerSize;
                        }

                        if (consumerCount >= 1 && (player instanceof Player)) {
                            if (Config.getConfig(((Player) player).getWorld()).PLAYER_COMMANDS) {
                                consumerCount = consumerCount - 1;
                            }
                        }

                        Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_CONSUMER, Color.WHITE, String.format("%,d", consumerCount), (consumerCount == 1 ? Selector.FIRST : Selector.SECOND)));
                    }
                    catch (Exception e) {
                        ErrorReporter.report(e);
                    }

                    long autoPurgeRowsPurged = ConfigHandler.autoPurgeRowsPurged.get();
                    Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_AUTO_PURGE, Color.WHITE, String.format("%,d", autoPurgeRowsPurged), (autoPurgeRowsPurged == 1 ? Selector.FIRST : Selector.SECOND)));

                    try {
                        String cpuInfo = "x" + Runtime.getRuntime().availableProcessors() + " " + Phrase.build(Phrase.CPU_CORES);
                        if (ConfigHandler.processorInfo != null) {
                            String modelName = ConfigHandler.processorInfo.getName();
                            if (modelName.contains(" CPU")) {
                                String[] split = modelName.split(" CPU")[0].split(" ");
                                modelName = split[split.length - 1];
                            }
                            else if (modelName.contains(" Processor")) {
                                String[] split = modelName.split(" Processor")[0].split(" ");
                                modelName = split[split.length - 1];
                            }

                            String cpuSpeed = String.valueOf(ConfigHandler.processorInfo.getMaxFrequency());
                            double speedVal = Long.valueOf(cpuSpeed) / 1000000000.0;

                            // Fix for Apple Silicon processors reporting 0 GHz
                            if (speedVal < 0.01 && SystemUtils.isAppleSilicon()) {
                                Double appleSiliconSpeed = SystemUtils.getAppleSiliconSpeed();
                                if (appleSiliconSpeed != null) {
                                    speedVal = appleSiliconSpeed;
                                }
                            }

                            cpuSpeed = String.format("%.2f", speedVal);
                            if (speedVal >= 0.01) {
                                cpuInfo = "x" + Runtime.getRuntime().availableProcessors() + " " + cpuSpeed + "GHz " + modelName + ".";
                            }
                        }

                        int mb = 1024 * 1024;
                        Runtime runtime = Runtime.getRuntime();
                        String usedRAM = String.format("%.2f", Double.valueOf((runtime.totalMemory() - runtime.freeMemory()) / mb) / 1000.0);
                        String totalRAM = String.format("%.2f", Double.valueOf(runtime.maxMemory() / mb) / 1000.0);
                        String systemInformation = Phrase.build(Phrase.RAM_STATS, usedRAM, totalRAM);
                        if (cpuInfo.length() > 0) {
                            systemInformation = cpuInfo + " (" + systemInformation + ")";
                        }

                        Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_SYSTEM, Color.WHITE, systemInformation));
                    }
                    catch (Exception e) {
                        ErrorReporter.report(e);
                    }

                    // Functions.sendMessage(player, Color.DARK_AQUA + "Website: " + Color.WHITE + "www.coreprotect.net/updates/");

                    // Functions.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.LINK_DISCORD, Color.WHITE + "www.coreprotect.net/discord/").replaceFirst(":", ":" + Color.WHITE));

                    if (player.isOp() && alert.get(player.getName()) == null) {
                        alert.put(player.getName(), true);

                        if (instance.getServer().getPluginManager().getPlugin("CoreEdit") == null) {
                            Thread.sleep(500);
                            /*
                            Functions.sendMessage(player, Color.WHITE + "----- " + Color.DARK_AQUA + "Recommended Plugin " + Color.WHITE + "-----");
                            Functions.sendMessage(player, Color.DARK_AQUA + "Notice: " + Color.WHITE + "Enjoy CoreProtect? Check out CoreEdit!");
                            Functions.sendMessage(player, Color.DARK_AQUA + "Download: " + Color.WHITE + "www.coreedit.net/download/");
                            */
                        }
                    }
                }
                catch (Exception e) {
                    ErrorReporter.report(e);
                }
            }
        }
        Runnable runnable = new BasicThread();
        Thread thread = new Thread(runnable);
        thread.start();
    }
    /**
     * Reports how much of the database holds recent, fully indexed activity and how much holds
     * compressed history. Only the SQLite storage keeps the two apart.
     *
     * @param player
     *            the command sender to report to
     */
    private static void sendStorageBreakdown(CommandSender player) {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return;
        }

        Connection connection = null;
        try {
            String importing = LegacyImport.getProgress();
            if (importing != null) {
                Chat.sendMessage(player, Color.DARK_AQUA + importing);
            }

            connection = Database.getConnection(true, 500);
            if (connection == null) {
                return;
            }

            ColdStorageStats stats = ColdStorageStats.read(connection);
            if (stats == null) {
                return;
            }

            Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_HOT_DATA, Color.WHITE, ColdStorageStats.format(stats.getHotBytes())));
            Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_COLD_DATA, Color.WHITE, ColdStorageStats.format(stats.getColdBytes()),
                    String.format("%,d", stats.getColdRows()), (stats.getColdRows() == 1 ? Selector.FIRST : Selector.SECOND)));
            if (stats.getBlobSavedBytes() > 0) {
                Chat.sendMessage(player, Color.DARK_AQUA + Phrase.build(Phrase.STATUS_ENTITY_DATA, Color.WHITE, ColdStorageStats.format(stats.getBlobSavedBytes())));
            }
        }
        catch (Exception e) {
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
        }
    }

}

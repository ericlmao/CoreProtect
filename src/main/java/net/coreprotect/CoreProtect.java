package net.coreprotect;

import java.io.File;
import java.nio.file.Path;

import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.language.Phrase;
import net.coreprotect.services.PluginInitializationService;
import net.coreprotect.services.ShutdownService;
import net.coreprotect.storage.LegacyStorageMigration;
import net.coreprotect.storage.StoragePaths;
import net.coreprotect.thread.Scheduler;
import net.coreprotect.utility.Chat;

/**
 * Main class for the CoreProtect plugin
 */
public final class CoreProtect extends JavaPlugin {

    private static CoreProtect instance;
    /**
     * Get the instance of CoreProtect
     *
     * @return This CoreProtect instance
     */
    public static CoreProtect getInstance() {
        return instance;
    }

    private final CoreProtectAPI api = new CoreProtectAPI();

    /**
     * Get the CoreProtect API
     *
     * @return The CoreProtect API
     */
    public CoreProtectAPI getAPI() {
        return api;
    }

    @Override
    public void onLoad() {
        // Set plugin instance and the two directories CoreProtect uses. The data folder holds
        // configuration; everything that is persisted lives in the server-root storage directory.
        instance = this;
        Path dataDirectory = this.getDataFolder().toPath();
        ConfigHandler.path = this.getDataFolder().getPath() + File.separator;
        ConfigHandler.storagePath = StoragePaths.root(dataDirectory);

        // Older versions kept databases and credentials in the data folder. They are moved before
        // anything reads the configuration, because loading it rewrites config.yml and would race
        // with the split of the database settings out of that same file.
        LegacyStorageMigration.run(dataDirectory, this.getLogger());
    }

    @Override
    public void onEnable() {
        // Folia hands work to the thread that owns the region it touches, so the schedulers have to
        // be ready before any listener or task is registered.
        Scheduler.initialize(this);

        // Initialize plugin using the initialization service
        boolean initialized = PluginInitializationService.initializePlugin(this);

        // Disable plugin if initialization failed
        if (!initialized) {
            Chat.console(Phrase.build(Phrase.ENABLE_FAILED, ConfigHandler.EDITION_NAME));
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        ShutdownService.safeShutdown(this);
    }

    public boolean isAdvancedChestsEnabled() {
        Plugin advancedChests = getServer().getPluginManager().getPlugin("AdvancedChests");
        return advancedChests != null && advancedChests.isEnabled();
    }
}

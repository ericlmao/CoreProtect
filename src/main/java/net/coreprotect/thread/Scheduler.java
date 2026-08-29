package net.coreprotect.thread;

import org.bukkit.World;
import org.bukkit.scheduler.BukkitTask;

import net.coreprotect.CoreProtect;
import net.coreprotect.config.ConfigHandler;

/**
 * Schedules CoreProtect's work, on Folia and on everything else.
 *
 * <p>
 * A normal server runs the whole world on one thread and takes tasks through Bukkit's scheduler. A
 * Folia server runs each region on a thread of its own, and a task has to be handed to the thread
 * that owns what it is about to touch. Callers say what their work concerns — a location, an entity,
 * a chunk, or nothing in particular — and this decides what that means on the server it is running
 * on.
 * </p>
 *
 * <p>
 * The Folia half lives in {@link FoliaScheduler}, which is only ever reached from behind the check
 * below, so a server without regionised scheduling never loads it or anything it refers to.
 * </p>
 */
public class Scheduler {

    private Scheduler() {
        throw new IllegalStateException("Scheduler class");
    }

    /**
     * Prepares the Folia schedulers, if this is a Folia server. Called once while the plugin is
     * enabling, before anything is scheduled.
     *
     * @param plugin
     *            the plugin the tasks are owned by
     */
    public static void initialize(CoreProtect plugin) {
        if (ConfigHandler.isFolia) {
            FoliaScheduler.init(plugin);
        }
    }

    public static void scheduleSyncDelayedTask(CoreProtect plugin, Runnable task, Object regionData, int delay) {
        scheduleSyncDelayedTask(plugin, task, null, regionData, delay);
    }

    public static void scheduleSyncDelayedTask(CoreProtect plugin, Runnable task, Runnable retiredTask, Object regionData, int delay) {
        if (ConfigHandler.isFolia) {
            FoliaScheduler.run(task, retiredTask, regionData, delay);
        }
        else if (delay == 0) {
            plugin.getServer().getScheduler().runTask(plugin, task);
        }
        else {
            plugin.getServer().getScheduler().runTaskLater(plugin, task, delay);
        }
    }

    /**
     * Runs a task on the thread that owns a chunk.
     *
     * <p>
     * On Folia this reaches the owning region without a location having to be invented to stand for
     * the chunk. Elsewhere a chunk owns nothing in particular, so the task is simply run on the
     * server thread.
     * </p>
     *
     * @param plugin
     *            the plugin the task is owned by
     * @param task
     *            the work to run
     * @param world
     *            the world the chunk is in
     * @param chunkX
     *            the chunk x coordinate
     * @param chunkZ
     *            the chunk z coordinate
     * @param delay
     *            ticks to wait, or 0 to run at the next opportunity
     */
    public static void scheduleForChunk(CoreProtect plugin, Runnable task, World world, int chunkX, int chunkZ, int delay) {
        if (ConfigHandler.isFolia) {
            FoliaScheduler.runForChunk(world, chunkX, chunkZ, task, delay);
        }
        else if (delay == 0) {
            plugin.getServer().getScheduler().runTask(plugin, task);
        }
        else {
            plugin.getServer().getScheduler().runTaskLater(plugin, task, delay);
        }
    }

    public static Object scheduleSyncRepeatingTask(CoreProtect plugin, Runnable task, Object regionData, int delay, int period) {
        if (ConfigHandler.isFolia) {
            return FoliaScheduler.runRepeating(task, regionData, delay, period);
        }
        return plugin.getServer().getScheduler().scheduleSyncRepeatingTask(plugin, task, delay, period);
    }

    public static void scheduleAsyncDelayedTask(CoreProtect plugin, Runnable task, int delay) {
        if (ConfigHandler.isFolia) {
            FoliaScheduler.runAsync(task, delay);
        }
        else if (delay == 0) {
            plugin.getServer().getScheduler().runTaskAsynchronously(plugin, task);
        }
        else {
            plugin.getServer().getScheduler().runTaskLaterAsynchronously(plugin, task, delay);
        }
    }

    public static void scheduleSyncDelayedTask(CoreProtect plugin, Runnable task, int delay) {
        scheduleSyncDelayedTask(plugin, task, null, delay);
    }

    public static void runTask(CoreProtect plugin, Runnable task) {
        scheduleSyncDelayedTask(plugin, task, null, 0);
    }

    public static void runTask(CoreProtect plugin, Runnable task, Object regionData) {
        scheduleSyncDelayedTask(plugin, task, regionData, 0);
    }

    public static void runTaskAsynchronously(CoreProtect plugin, Runnable task) {
        scheduleAsyncDelayedTask(plugin, task, 0);
    }

    public static void runTaskLaterAsynchronously(CoreProtect plugin, Runnable task, int delay) {
        scheduleAsyncDelayedTask(plugin, task, delay);
    }

    public static void cancelTask(Object task) {
        if (ConfigHandler.isFolia) {
            FoliaScheduler.cancel(task);
        }
        else if (task instanceof BukkitTask) {
            ((BukkitTask) task).cancel();
        }
    }
}

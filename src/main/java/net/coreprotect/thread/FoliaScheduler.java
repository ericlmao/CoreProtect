package net.coreprotect.thread;

import java.util.concurrent.TimeUnit;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.java.JavaPlugin;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;

/**
 * Every call into Folia's regionised schedulers, in one place.
 *
 * <p>
 * Folia runs each region of the world on its own thread, so a task has to be handed to the thread
 * that owns whatever it is about to touch: the region a location sits in, the region an entity is
 * currently in, or the global region for work that belongs to no place in particular. Getting that
 * wrong is not a slow path, it is a thread safety error.
 * </p>
 *
 * <p>
 * The choice is made by the folia-scheduler library rather than by hand here. This class exists to
 * keep every reference to it, and to Folia's own scheduler types, inside one class that a server
 * without regionised scheduling never loads. Those classes are compiled for a newer Java release
 * than CoreProtect targets, which is safe precisely because a server old enough to matter has no
 * Folia to hand them to; {@link Scheduler} reaches this class only once it knows it is on Folia.
 * </p>
 */
final class FoliaScheduler {

    private FoliaScheduler() {
        throw new IllegalStateException("Scheduler class");
    }

    /**
     * Prepares the schedulers. Called once while the plugin is enabling, before anything is
     * scheduled.
     *
     * @param plugin
     *            the plugin the tasks are owned by
     */
    static void init(JavaPlugin plugin) {
        gg.moonrise.scheduler.Scheduler.init(plugin);
    }

    /**
     * Runs a task on the thread that owns whatever it is about to touch.
     *
     * @param task
     *            the work to run
     * @param retiredTask
     *            run instead when the task was tied to an entity that has since been removed, or
     *            null to let it be dropped
     * @param regionData
     *            a location, an entity, or null for the global region
     * @param delay
     *            ticks to wait, or 0 to run at the next opportunity
     */
    static void run(Runnable task, Runnable retiredTask, Object regionData, int delay) {
        if (regionData instanceof Location) {
            Location location = (Location) regionData;
            if (delay == 0) {
                gg.moonrise.scheduler.Scheduler.location().run(location, scheduled -> task.run());
            }
            else {
                gg.moonrise.scheduler.Scheduler.location().runDelayed(location, delay, scheduled -> task.run());
            }
        }
        else if (regionData instanceof Entity) {
            Entity entity = (Entity) regionData;
            if (delay == 0) {
                gg.moonrise.scheduler.Scheduler.entity(entity).run(scheduled -> task.run(), retiredTask);
            }
            else {
                gg.moonrise.scheduler.Scheduler.entity(entity).runDelayed(scheduled -> task.run(), retiredTask, delay);
            }
        }
        else {
            if (delay == 0) {
                gg.moonrise.scheduler.Scheduler.sync().run(scheduled -> task.run());
            }
            else {
                gg.moonrise.scheduler.Scheduler.sync().runDelayed(scheduled -> task.run(), delay);
            }
        }
    }

    /**
     * Runs a task on the thread that owns a chunk, without building a location to stand for it.
     *
     * @param world
     *            the world the chunk is in
     * @param chunkX
     *            the chunk x coordinate
     * @param chunkZ
     *            the chunk z coordinate
     * @param task
     *            the work to run
     * @param delay
     *            ticks to wait, or 0 to run at the next opportunity
     */
    static void runForChunk(World world, int chunkX, int chunkZ, Runnable task, int delay) {
        if (delay == 0) {
            gg.moonrise.scheduler.Scheduler.location().run(world, chunkX, chunkZ, scheduled -> task.run());
        }
        else {
            gg.moonrise.scheduler.Scheduler.location().runDelayed(world, chunkX, chunkZ, delay, scheduled -> task.run());
        }
    }

    /**
     * Repeats a task on the thread that owns whatever it touches.
     *
     * @param task
     *            the work to run
     * @param regionData
     *            a location, an entity, or null for the global region
     * @param delay
     *            ticks before the first run
     * @param period
     *            ticks between runs
     * @return the scheduled task, for cancelling it later
     */
    static Object runRepeating(Runnable task, Object regionData, int delay, int period) {
        if (regionData instanceof Location) {
            return gg.moonrise.scheduler.Scheduler.location().schedule((Location) regionData, delay, period, scheduled -> task.run());
        }
        if (regionData instanceof Entity) {
            return gg.moonrise.scheduler.Scheduler.entity((Entity) regionData).schedule(scheduled -> task.run(), delay, period);
        }
        return gg.moonrise.scheduler.Scheduler.sync().schedule(scheduled -> task.run(), delay, period);
    }

    /**
     * Runs a task off the server's threads entirely.
     *
     * @param task
     *            the work to run
     * @param delay
     *            ticks to wait, or 0 to run now
     */
    static void runAsync(Runnable task, int delay) {
        if (delay == 0) {
            gg.moonrise.scheduler.Scheduler.async().run(scheduled -> task.run());
        }
        else {
            // Off the tick loop there are no ticks to count, so the wait is expressed as the time
            // that many ticks would have taken.
            gg.moonrise.scheduler.Scheduler.async().runDelayed(scheduled -> task.run(), delay * 50L, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * @param task
     *            a task returned by one of the methods here
     * @return true when the task was recognised and cancelled
     */
    static boolean cancel(Object task) {
        if (task instanceof ScheduledTask) {
            ((ScheduledTask) task).cancel();
            return true;
        }
        return false;
    }
}

package net.coreprotect.database;

import java.text.NumberFormat;

/**
 * What compacting is doing at the moment, so a run that takes minutes can say so.
 *
 * <p>
 * Compacting a large database is a long job made of several phases, and until it finishes there is
 * nothing to tell an operator whether it is working or stuck. Each phase records what it is on and
 * how far through it is; whoever asked for the compact reads that every so often and reports it.
 * </p>
 *
 * <p>
 * Only one compact runs at a time, which is what lets this be shared rather than passed from hand to
 * hand through every method that does a piece of the work.
 * </p>
 */
public final class CompactProgress {

    private static volatile String phase;
    private static volatile long done;
    private static volatile long total;

    private CompactProgress() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Records what is happening now.
     *
     * @param phase
     *            what is being done, in words
     * @param done
     *            how much of it is finished
     * @param total
     *            how much there is, or 0 when that is not known ahead of time
     */
    public static void set(String phase, long done, long total) {
        CompactProgress.phase = phase;
        CompactProgress.done = done;
        CompactProgress.total = total;
    }

    /**
     * Forgets what was happening, once nothing is.
     */
    public static void clear() {
        phase = null;
        done = 0;
        total = 0;
    }

    /**
     * @return what to tell an operator, or null when nothing is being done
     */
    public static String line() {
        String current = phase;
        if (current == null) {
            return null;
        }

        long finished = done;
        long expected = total;
        if (expected > 0) {
            long percent = Math.min(100, (finished * 100L) / expected);
            return current + " (" + percent + "%)";
        }
        if (finished > 0) {
            return current + " (" + NumberFormat.getInstance().format(finished) + ")";
        }
        return current;
    }
}

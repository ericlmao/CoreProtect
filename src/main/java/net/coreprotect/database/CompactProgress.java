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
    private static volatile String detail;

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
        CompactProgress.detail = null;
    }

    /**
     * Records what is happening now, saying how far along in its own terms.
     *
     * <p>
     * A share of the whole is only worth reporting when the whole is small enough for the share to
     * move. Returning freed space works through millions of pages a few thousand at a time, so a
     * percentage sits on nought for minutes while a size does not.
     * </p>
     *
     * @param phase
     *            what is being done, in words
     * @param detail
     *            how far along, already in words
     */
    public static void set(String phase, String detail) {
        CompactProgress.phase = phase;
        CompactProgress.detail = detail;
        CompactProgress.done = 0;
        CompactProgress.total = 0;
    }

    /**
     * Forgets what was happening, once nothing is.
     */
    public static void clear() {
        phase = null;
        detail = null;
        done = 0;
        total = 0;
    }

    /**
     * A length of time in words, short enough to sit at the end of a progress line.
     *
     * <p>
     * Rounded to the largest two units that say anything, so an hour long wait reads as hours and
     * minutes rather than as thousands of seconds.
     * </p>
     *
     * @param milliseconds
     *            how long
     * @return the same, in words
     */
    public static String duration(long milliseconds) {
        long seconds = Math.max(0, milliseconds) / 1000L;
        long hours = seconds / 3600L;
        long minutes = (seconds % 3600L) / 60L;
        if (hours > 0) {
            return hours + "h " + minutes + "m";
        }
        if (minutes > 0) {
            return minutes + "m " + (seconds % 60L) + "s";
        }
        return seconds > 0 ? seconds + "s" : "<1s";
    }

    /**
     * @return what to tell an operator, or null when nothing is being done
     */
    public static String line() {
        String current = phase;
        if (current == null) {
            return null;
        }

        String described = detail;
        if (described != null) {
            return current + " (" + described + ")";
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

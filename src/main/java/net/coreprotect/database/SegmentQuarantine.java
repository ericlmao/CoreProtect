package net.coreprotect.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.coreprotect.utility.Chat;
import net.coreprotect.utility.ErrorReporter;

/**
 * Remembers the segments that background maintenance could not rewrite, so it moves on to the rest.
 *
 * <p>
 * Rewriting a segment into the current format, or merging a day's segments, reads every row of it. A
 * segment that cannot be read would otherwise fail the same way on every pass: the work behind it
 * would never be done, and the same error would be reported every thirty seconds for as long as the
 * server runs. A segment set aside here is left exactly as it is, still read by lookups as before,
 * and is tried again after a restart, when whatever went wrong may have been put right.
 * </p>
 */
public final class SegmentQuarantine {

    private static final Set<Long> SEGMENTS = ConcurrentHashMap.newKeySet();

    /** The failures already reported in full, so a fault shared by many segments is reported once. */
    private static final Set<String> REPORTED = ConcurrentHashMap.newKeySet();

    private SegmentQuarantine() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Sets segments aside after maintenance failed on them.
     *
     * @param ids
     *            the segments the failed work was reading
     * @param work
     *            what was being done, for the console
     * @param failure
     *            what went wrong
     */
    static void add(Collection<Long> ids, String work, Throwable failure) {
        if (ids.isEmpty()) {
            return;
        }
        SEGMENTS.addAll(ids);
        String signature = failure.getClass().getName() + ": " + failure.getMessage();
        if (REPORTED.add(signature)) {
            Chat.console("[CoreProtect] Could not finish " + work + " for " + ids.size() + (ids.size() == 1 ? " segment" : " segments")
                    + " of compressed storage, which will be left as they are until the next restart. Lookups still read them.");
            ErrorReporter.report(failure);
        }
        else {
            SQLiteColdIndex.debug(work + ": set aside " + ids.size() + " more segments after the same failure (" + signature + ")");
        }
    }

    /**
     * @param id
     *            a segment id
     * @return true if maintenance has set this segment aside
     */
    static boolean contains(long id) {
        return SEGMENTS.contains(id);
    }

    /**
     * @return the segments set aside, as a comma separated list for a query, or null when there are
     *         none
     */
    static String sqlList() {
        if (SEGMENTS.isEmpty()) {
            return null;
        }
        List<String> ids = new ArrayList<>();
        for (Long id : SEGMENTS) {
            ids.add(Long.toString(id));
        }
        return String.join(",", ids);
    }

    /**
     * @return how many segments are set aside
     */
    static int size() {
        return SEGMENTS.size();
    }

    /**
     * Forgets every segment set aside. Segment ids are only meaningful within one database, so this
     * is done whenever the metadata of the database is let go of.
     */
    static void clear() {
        SEGMENTS.clear();
        REPORTED.clear();
    }

    /**
     * Tells failures that say something about a segment apart from ones that only say the database
     * was busy or the work was asked to stop. Only the first kind is worth setting a segment aside
     * for; the second is simply tried again later.
     *
     * @param failure
     *            what went wrong
     * @return true if the failure is about the segment being worked on
     */
    static boolean concernsSegment(Throwable failure) {
        if (failure instanceof InterruptedException || failure instanceof ColdRollupTask.SealDeferred || failure instanceof SegmentCompactor.RunChanged) {
            return false;
        }
        return !Database.isLocked(failure);
    }
}

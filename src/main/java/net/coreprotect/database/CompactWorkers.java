package net.coreprotect.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Runs the parts of a compact that are pure computation on more than one core.
 *
 * <p>
 * A compact spends most of its time compressing: rows are packed into segments and entity blobs into
 * groups, both at the level used for long term storage, which is deliberately slow because the
 * result is written once and read for years. Everything else about a compact is bound by the
 * database, which allows one writer and cannot be made parallel, but compression happens outside any
 * transaction and depends on nothing but the bytes handed to it. That part scales with cores.
 * </p>
 *
 * <p>
 * One core is left free. A compact runs on a live server, and taking every core would cost the
 * server the ticks it needs for everything else it is doing.
 * </p>
 */
public final class CompactWorkers {

    /** How many pieces of work run at once when the server has not been told to use a set number. */
    private static final int DEFAULT_THREADS = Math.max(1, Math.min(6, Runtime.getRuntime().availableProcessors() - 1));

    /** Started when a compact first needs it, and let go again once a compact stops asking. */
    private static ThreadPoolExecutor pool;

    private CompactWorkers() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * A piece of work that turns one thing into another and may fail.
     *
     * @param <S>
     *            what goes in
     * @param <T>
     *            what comes out
     */
    public interface Work<S, T> {
        T apply(S item) throws Exception;
    }

    /**
     * @return how many pieces of work run at once
     */
    public static int threads() {
        int configured = net.coreprotect.config.Config.getGlobal().COMPACT_THREADS;
        return configured > 0 ? configured : DEFAULT_THREADS;
    }

    /**
     * Applies the same work to every item, on as many cores as are spare.
     *
     * <p>
     * Results come back in the order the items were given, whatever order they finished in, because
     * what is done with them afterwards is written to the database in that order.
     * </p>
     *
     * @param items
     *            what to work through
     * @param work
     *            what to do with each
     * @param <S>
     *            what goes in
     * @param <T>
     *            what comes out
     * @return the results, in the order of the items
     * @throws Exception
     *             the first failure, once the rest have been abandoned
     */
    public static <S, T> List<T> map(List<S> items, Work<S, T> work) throws Exception {
        int threads = threads();
        List<T> results = new ArrayList<>(items.size());
        if (items.size() < 2 || threads < 2) {
            for (S item : items) {
                results.add(work.apply(item));
            }
            return results;
        }

        List<Future<T>> pending = new ArrayList<>(items.size());
        ThreadPoolExecutor executor = executor(threads);
        for (S item : items) {
            pending.add(executor.submit(() -> work.apply(item)));
        }

        try {
            for (Future<T> future : pending) {
                results.add(future.get());
            }
        }
        catch (ExecutionException failure) {
            for (Future<T> future : pending) {
                future.cancel(true);
            }
            Throwable cause = failure.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new IllegalStateException(cause);
        }
        return results;
    }

    private static synchronized ThreadPoolExecutor executor(int threads) {
        if (pool != null && pool.getMaximumPoolSize() != threads) {
            // The server was told to use a different number since the last compact.
            pool.shutdown();
            pool = null;
        }
        if (pool == null) {
            pool = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
                Thread thread = new Thread(runnable, "CoreProtect-Compact-Worker");
                thread.setDaemon(true);
                // Below the threads doing the server's own work, since this is housekeeping.
                thread.setPriority(Thread.NORM_PRIORITY - 1);
                return thread;
            });
            // The threads go away again when a compact finishes, rather than sitting idle for the
            // life of the server.
            pool.allowCoreThreadTimeOut(true);
        }
        return pool;
    }
}

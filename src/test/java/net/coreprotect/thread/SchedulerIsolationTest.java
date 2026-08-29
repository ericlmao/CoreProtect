package net.coreprotect.thread;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * The gate for keeping Folia's schedulers off servers that do not have them.
 *
 * <p>
 * The scheduler library is compiled for a newer Java release than CoreProtect targets. That is only
 * safe because nothing reaches it except through a check for a Folia server, and a server old enough
 * for the Java release to matter is not one. If a call to it ever moves out from behind that check,
 * an older server stops being able to load the class that holds it, which is the kind of fault that
 * shows up as the plugin failing to start rather than as a test going red.
 * </p>
 *
 * <p>
 * So the check is made here instead: {@link Scheduler} is loaded by a class loader that refuses to
 * hand over anything from the library, and is then used the way a server without Folia would use it.
 * </p>
 */
class SchedulerIsolationTest {

    /** The package the library sits in before the build relocates it. */
    private static final String LIBRARY_PACKAGE = "gg.moonrise.";

    static boolean isCompiled() {
        return Files.isDirectory(classesDirectory());
    }

    private static Path classesDirectory() {
        return Paths.get("target", "classes");
    }

    /**
     * A class loader that has never heard of the scheduler library, and that loads CoreProtect's own
     * classes itself so that what they refer to is resolved through it rather than around it.
     */
    private static final class WithoutLibrary extends URLClassLoader {

        private WithoutLibrary(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(LIBRARY_PACKAGE)) {
                throw new ClassNotFoundException(name);
            }
            if (!name.startsWith("net.coreprotect.")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = findClass(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }
    }

    private static WithoutLibrary loader() throws Exception {
        URL classes = classesDirectory().toUri().toURL();
        return new WithoutLibrary(new URL[] { classes }, SchedulerIsolationTest.class.getClassLoader());
    }

    @Test
    @EnabledIf("isCompiled")
    void theSchedulerWorksWithoutTheLibraryOnTheClassPath() throws Exception {
        try (WithoutLibrary loader = loader()) {
            Class<?> scheduler = Class.forName("net.coreprotect.thread.Scheduler", true, loader);
            assertNotNull(scheduler, "the scheduler loads without the library");
            assertEquals(loader, scheduler.getClassLoader(), "the scheduler was loaded by the loader under test");

            // What a server without Folia does with it. None of this may reach the library.
            Method cancelTask = scheduler.getMethod("cancelTask", Object.class);
            cancelTask.invoke(null, new Object());
            cancelTask.invoke(null, (Object) null);
        }
    }

    @Test
    @EnabledIf("isCompiled")
    void theLibraryReallyIsOutOfReachOfThatLoader() throws Exception {
        // Without this, the test above would pass just as well if the loader let everything through.
        try (WithoutLibrary loader = loader()) {
            assertThrows(ClassNotFoundException.class,
                    () -> Class.forName(LIBRARY_PACKAGE + "scheduler.Scheduler", true, loader),
                    "the loader hides the library");

            // The class that calls into the library still loads: the references in it are not
            // resolved until one of them is reached. Reaching one is what has to fail.
            Class<?> folia = Class.forName("net.coreprotect.thread.FoliaScheduler", true, loader);
            Method run = folia.getDeclaredMethod("run", Runnable.class, Runnable.class, Object.class, int.class);
            run.setAccessible(true);

            InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                    () -> run.invoke(null, (Runnable) () -> {
                    }, null, null, 0),
                    "scheduling through it needs the library");
            assertTrue(thrown.getCause() instanceof NoClassDefFoundError,
                    "it fails for want of the library, not for some other reason: " + thrown.getCause());
        }
    }

    @Test
    @EnabledIf("isCompiled")
    void everyCallIntoTheLibraryIsInTheOneClass() throws Exception {
        // The isolation only holds while the calls stay put, and a stray call elsewhere would only be
        // noticed on a server too old to load it. The compiled classes are read for the library's name.
        Path classes = classesDirectory();
        StringBuilder offenders = new StringBuilder();
        byte[] marker = "gg/moonrise/".getBytes("UTF-8");

        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".class"))::iterator) {
                String name = classes.relativize(file).toString().replace(File.separatorChar, '/');
                if (name.startsWith("net/coreprotect/thread/FoliaScheduler")) {
                    continue;
                }
                if (contains(Files.readAllBytes(file), marker)) {
                    offenders.append(offenders.length() > 0 ? ", " : "").append(name);
                }
            }
        }

        assertEquals("", offenders.toString(), "only FoliaScheduler may name the scheduler library");
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer: for (int start = 0; start + needle.length <= haystack.length; start++) {
            for (int index = 0; index < needle.length; index++) {
                if (haystack[start + index] != needle[index]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }
}

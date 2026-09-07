package net.coreprotect.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Primitives that move persisted files out of the plugin data folder and into the server-root
 * storage directory without losing data.
 *
 * <p>
 * Every operation is idempotent so it can run on each boot. A file that only exists at the legacy
 * location is moved. When both locations hold a file, the file already in storage wins and the
 * legacy file is renamed aside with a timestamp instead of being deleted, so an operator can always
 * recover it.
 * </p>
 */
public final class StorageRelocation {

    private static final String STAGING_SUFFIX = ".migrating";
    private static final String INCOMPLETE_SUFFIX = ".migration-incomplete";
    private static final String SHELVED_INFIX = ".legacy-";

    private StorageRelocation() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Severity used when a legacy file has to be shelved because storage already owns the same file.
     */
    public enum ConflictSeverity {

        /** Recoverable state such as a database or a cache. */
        WARNING,

        /** Operator credentials, where booting on the storage copy can mean connecting nowhere. */
        ERROR
    }

    /**
     * Moves a single legacy file into storage.
     *
     * @param legacy
     *            the legacy path inside the plugin data folder
     * @param target
     *            the owning path inside the storage directory
     * @param log
     *            the plugin logger
     */
    public static void relocateFile(Path legacy, Path target, Logger log) {
        relocateFile(legacy, target, log, ConflictSeverity.WARNING);
    }

    /**
     * Moves a single legacy file into storage.
     *
     * @param legacy
     *            the legacy path inside the plugin data folder
     * @param target
     *            the owning path inside the storage directory
     * @param log
     *            the plugin logger
     * @param severity
     *            the log severity used when both locations hold the file
     */
    public static void relocateFile(Path legacy, Path target, Logger log, ConflictSeverity severity) {
        Objects.requireNonNull(legacy, "legacy");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(severity, "severity");
        if (!Files.isRegularFile(legacy)) {
            return;
        }
        if (Files.exists(target)) {
            shelve(legacy, log, severity, target);
            return;
        }

        createParent(target);
        move(legacy, target);
        log.info("Moved " + legacy + " to " + target + ".");
    }

    /**
     * Moves a legacy database and any write-ahead sidecars into storage as one unit.
     *
     * <p>
     * A clean shutdown checkpoints and removes the sidecars, so the common case is a single atomic
     * move. When sidecars do exist, a marker file guards the multi-file window: finding that marker
     * on a later boot means a previous migration was interrupted, and startup fails closed rather
     * than opening a database that may be missing committed transactions.
     * </p>
     *
     * @param legacyDatabase
     *            the legacy database path inside the plugin data folder
     * @param targetDatabase
     *            the owning database path inside the storage directory
     * @param sidecarSuffixes
     *            the suffixes the engine appends to the database file name
     * @param log
     *            the plugin logger
     */
    public static void relocateDatabase(Path legacyDatabase, Path targetDatabase, List<String> sidecarSuffixes, Logger log) {
        Objects.requireNonNull(legacyDatabase, "legacyDatabase");
        Objects.requireNonNull(targetDatabase, "targetDatabase");
        Objects.requireNonNull(sidecarSuffixes, "sidecarSuffixes");
        Objects.requireNonNull(log, "log");

        Path marker = suffixed(targetDatabase, INCOMPLETE_SUFFIX);
        if (Files.exists(marker)) {
            throw new IllegalStateException("A previous storage migration of " + targetDatabase + " did not finish."
                    + " Database parts may be split between " + legacyDatabase.getParent() + " and "
                    + targetDatabase.getParent() + ". Reunite the database file with its " + String.join(", ", sidecarSuffixes)
                    + " sidecars in one directory, then delete " + marker + " to continue.");
        }
        if (!Files.isRegularFile(legacyDatabase)) {
            return;
        }
        if (Files.exists(targetDatabase)) {
            String stamp = timestamp(legacyDatabase);
            for (Path sidecar : existingSidecars(legacyDatabase, sidecarSuffixes)) {
                shelveTo(sidecar, shelvedName(sidecar, stamp));
            }
            shelve(legacyDatabase, log, ConflictSeverity.WARNING, targetDatabase);
            return;
        }

        createParent(targetDatabase);
        List<Path> sidecars = existingSidecars(legacyDatabase, sidecarSuffixes);
        if (sidecars.isEmpty()) {
            move(legacyDatabase, targetDatabase);
            log.info("Moved " + legacyDatabase + " to " + targetDatabase + ".");
            return;
        }

        createFile(marker);
        move(legacyDatabase, targetDatabase);
        int nameLength = legacyDatabase.getFileName().toString().length();
        for (Path sidecar : sidecars) {
            String suffix = sidecar.getFileName().toString().substring(nameLength);
            move(sidecar, suffixed(targetDatabase, suffix));
        }
        delete(marker);
        log.info("Moved " + legacyDatabase + " and " + sidecars.size() + " write-ahead sidecar(s) to " + targetDatabase + ".");
    }

    /**
     * Moves every legacy file whose name matches a glob into storage.
     *
     * <p>
     * Used for the dated copies CoreProtect sets aside itself, such as the database an upgrade
     * renamed out of the way, whose exact names are only known by pattern.
     * </p>
     *
     * @param legacyDirectory
     *            the legacy directory inside the plugin data folder
     * @param glob
     *            the file name pattern to move
     * @param targetDirectory
     *            the owning directory inside the storage directory
     * @param log
     *            the plugin logger
     */
    public static void relocateMatching(Path legacyDirectory, String glob, Path targetDirectory, Logger log) {
        Objects.requireNonNull(legacyDirectory, "legacyDirectory");
        Objects.requireNonNull(glob, "glob");
        Objects.requireNonNull(targetDirectory, "targetDirectory");
        Objects.requireNonNull(log, "log");
        if (!Files.isDirectory(legacyDirectory)) {
            return;
        }

        List<Path> matches = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(legacyDirectory, glob)) {
            for (Path entry : entries) {
                if (Files.isRegularFile(entry) && isMigratable(entry)) {
                    matches.add(entry);
                }
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to inspect legacy directory " + legacyDirectory, exception);
        }

        Collections.sort(matches);
        for (Path match : matches) {
            relocateFile(match, targetDirectory.resolve(match.getFileName().toString()), log);
        }
    }

    /**
     * Merges a legacy directory into storage file by file.
     *
     * <p>
     * Merging per file rather than per directory keeps legacy entries that storage does not have
     * yet, which matters for directories written into over time.
     * </p>
     *
     * @param legacyDirectory
     *            the legacy directory inside the plugin data folder
     * @param targetDirectory
     *            the owning directory inside the storage directory
     * @param log
     *            the plugin logger
     */
    public static void mergeDirectory(Path legacyDirectory, Path targetDirectory, Logger log) {
        Objects.requireNonNull(legacyDirectory, "legacyDirectory");
        Objects.requireNonNull(targetDirectory, "targetDirectory");
        Objects.requireNonNull(log, "log");
        if (!Files.isDirectory(legacyDirectory)) {
            return;
        }

        List<Path> files;
        try (Stream<Path> entries = Files.walk(legacyDirectory)) {
            // Files this migration shelved on an earlier boot stay put: moving them would defeat
            // the recovery path an operator was told to look at, and would re-enter live storage.
            files = entries.filter(Files::isRegularFile).filter(StorageRelocation::isMigratable)
                    .collect(Collectors.toList());
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to inspect legacy directory " + legacyDirectory, exception);
        }

        for (Path file : files) {
            relocateFile(file, targetDirectory.resolve(legacyDirectory.relativize(file).toString()), log);
        }
        deleteEmptyTree(legacyDirectory);
    }

    /**
     * Renames a file aside with a timestamp, leaving it where an operator can recover it.
     *
     * @param legacy
     *            the file to set aside
     * @return the path the file was renamed to
     */
    public static Path shelveAside(Path legacy) {
        Path shelved = shelvedName(legacy, timestamp(legacy));
        shelveTo(legacy, shelved);
        return shelved;
    }

    private static boolean isMigratable(Path file) {
        String name = file.getFileName().toString();
        return !name.contains(SHELVED_INFIX) && !name.endsWith(INCOMPLETE_SUFFIX) && !name.endsWith(STAGING_SUFFIX);
    }

    private static List<Path> existingSidecars(Path database, List<String> sidecarSuffixes) {
        List<Path> sidecars = new ArrayList<>();
        for (String suffix : sidecarSuffixes) {
            Path sidecar = suffixed(database, suffix);
            if (Files.isRegularFile(sidecar)) {
                sidecars.add(sidecar);
            }
        }
        return Collections.unmodifiableList(sidecars);
    }

    private static void shelve(Path legacy, Logger log, ConflictSeverity severity, Path target) {
        Path shelved = shelveAside(legacy);
        String message = target + " already exists, so the legacy file " + legacy + " was not used. It was renamed to "
                + shelved + " and can be deleted once its contents are confirmed obsolete.";
        log.log(severity == ConflictSeverity.ERROR ? Level.SEVERE : Level.WARNING, message);
    }

    private static void shelveTo(Path legacy, Path shelved) {
        try {
            Files.move(legacy, shelved, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException exception) {
            try {
                Files.move(legacy, shelved);
            }
            catch (IOException fallbackException) {
                throw new UncheckedIOException("Failed to rename legacy file " + legacy + " aside", fallbackException);
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to rename legacy file " + legacy + " aside", exception);
        }
    }

    private static void move(Path source, Path target) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException exception) {
            copyThenDelete(source, target);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to move " + source + " to " + target, exception);
        }
    }

    /**
     * Copies across a filesystem boundary through a staging file so an interrupted copy can never
     * leave a partial file at the owning path, where it would outrank the intact legacy file.
     */
    private static void copyThenDelete(Path source, Path target) {
        Path staging = suffixed(target, STAGING_SUFFIX);
        try {
            Files.deleteIfExists(staging);
            Files.copy(source, staging, StandardCopyOption.COPY_ATTRIBUTES);
            try (FileChannel channel = FileChannel.open(staging, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            }
            catch (AtomicMoveNotSupportedException exception) {
                Files.move(staging, target);
            }
            Files.delete(source);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to copy " + source + " to " + target, exception);
        }
    }

    private static void createParent(Path target) {
        Path parent = target.getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to create storage directory " + parent, exception);
        }
    }

    private static void createFile(Path marker) {
        try {
            Files.createFile(marker);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to create migration marker " + marker, exception);
        }
    }

    private static void delete(Path marker) {
        try {
            Files.delete(marker);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to delete migration marker " + marker, exception);
        }
    }

    private static void deleteEmptyTree(Path directory) {
        List<Path> directories;
        try (Stream<Path> entries = Files.walk(directory)) {
            directories = entries.filter(Files::isDirectory)
                    .sorted((left, right) -> right.getNameCount() - left.getNameCount()).collect(Collectors.toList());
        }
        catch (IOException exception) {
            return;
        }

        for (Path candidate : directories) {
            try {
                Files.deleteIfExists(candidate);
            }
            catch (IOException exception) {
                return;
            }
        }
    }

    private static Path suffixed(Path path, String suffix) {
        return path.resolveSibling(path.getFileName().toString() + suffix);
    }

    /**
     * Names the shelved copy after the legacy file's own last-modified time, which identifies the
     * data without reading a clock, and disambiguates the rare case where that name is taken.
     */
    private static Path shelvedName(Path legacy, String stamp) {
        Path candidate = suffixed(legacy, SHELVED_INFIX + stamp);
        int attempt = 1;
        while (Files.exists(candidate)) {
            candidate = suffixed(legacy, SHELVED_INFIX + stamp + "-" + attempt);
            attempt++;
        }
        return candidate;
    }

    private static String timestamp(Path legacy) {
        try {
            return Long.toString(Files.getLastModifiedTime(legacy).toMillis());
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read the modification time of " + legacy, exception);
        }
    }
}

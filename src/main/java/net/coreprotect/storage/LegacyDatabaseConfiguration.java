package net.coreprotect.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Splits the database engine selection and its credentials out of {@code config.yml} and into
 * {@code database.yml} inside the storage directory.
 *
 * <p>
 * A plugin data folder is routinely copied between servers, zipped up for support, and committed to
 * configuration management. The keys that name a MySQL or ClickHouse server and hold its password
 * do not belong there, so they move to storage along with the databases themselves.
 * </p>
 *
 * <p>
 * The split is textual rather than a parse and re-emit, so an operator's own comments, ordering,
 * and spacing survive it. Each key travels with the comment block written immediately above it,
 * which is where CoreProtect puts the explanation of what the key does.
 * </p>
 */
public final class LegacyDatabaseConfiguration {

    /** Written as the first line of a newly created database configuration. */
    public static final String FILE_HEADER = "# CoreProtect Database Config";

    private static final String STAGING_SUFFIX = ".migrating";

    private LegacyDatabaseConfiguration() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Moves every database key out of the plugin configuration and into the storage directory.
     *
     * <p>
     * Nothing happens when the configuration holds no database keys, which is the case on a fresh
     * installation and on every boot after the first migrated one. When storage already owns a
     * database configuration and the plugin configuration still carries database keys, the storage
     * copy is authoritative: the keys found in the plugin folder are written to a dated file beside
     * it rather than being discarded, and the operator is told where to find them.
     * </p>
     *
     * @param configuration
     *            the plugin configuration, normally {@code plugins/CoreProtect/config.yml}
     * @param databaseConfiguration
     *            the owning path inside the storage directory
     * @param log
     *            the plugin logger
     */
    public static void split(Path configuration, Path databaseConfiguration, Logger log) {
        if (!Files.isRegularFile(configuration)) {
            return;
        }

        List<String> lines = read(configuration);
        Split split = partition(lines);
        if (split.database.isEmpty()) {
            return;
        }

        if (Files.exists(databaseConfiguration)) {
            Path shelved = shelveExtracted(configuration, split.database);
            log.log(Level.SEVERE, databaseConfiguration + " already exists, so the database settings still present in "
                    + configuration + " were not used. They were written to " + shelved
                    + " and removed from the plugin configuration. Check that " + databaseConfiguration
                    + " names the database this server should be writing to before deleting that file.");
            write(configuration, split.configuration);
            return;
        }

        List<String> contents = new ArrayList<>();
        contents.add(FILE_HEADER);
        contents.addAll(split.database);
        createParent(databaseConfiguration);
        write(databaseConfiguration, contents);
        write(configuration, split.configuration);
        log.info("Moved the database settings out of " + configuration + " and into " + databaseConfiguration + ".");
    }

    /**
     * Splits configuration lines into the ones that stay and the ones that move.
     *
     * <p>
     * Blank lines and comments are held back until the key they introduce is read, so that a key's
     * explanation moves with it. The file's own opening comment is not held back, because it
     * describes the file rather than the first key in it.
     * </p>
     */
    static Split partition(List<String> lines) {
        List<String> configuration = new ArrayList<>();
        List<String> database = new ArrayList<>();
        List<String> pending = new ArrayList<>();

        int start = 0;
        if (!lines.isEmpty() && lines.get(0).trim().startsWith("#")) {
            configuration.add(lines.get(0));
            start = 1;
        }

        for (int index = start; index < lines.size(); index++) {
            String line = lines.get(index);
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                pending.add(line);
                continue;
            }

            List<String> destination = StorageFiles.DATABASE_KEYS.contains(key(trimmed)) ? database : configuration;
            destination.addAll(pending);
            destination.add(line);
            pending.clear();
        }

        configuration.addAll(pending);
        return new Split(configuration, database);
    }

    /**
     * @return the key a configuration line sets, or null when the line sets nothing
     */
    private static String key(String trimmed) {
        int separator = trimmed.indexOf(':');
        return separator == -1 ? null : trimmed.substring(0, separator).trim();
    }

    private static Path shelveExtracted(Path configuration, List<String> database) {
        List<String> contents = new ArrayList<>();
        contents.add(FILE_HEADER);
        contents.addAll(database);

        Path shelved = configuration.resolveSibling(configuration.getFileName() + ".database-legacy");
        int attempt = 1;
        while (Files.exists(shelved)) {
            shelved = configuration.resolveSibling(configuration.getFileName() + ".database-legacy-" + attempt);
            attempt++;
        }
        write(shelved, contents);
        return shelved;
    }

    private static List<String> read(Path file) {
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read " + file, exception);
        }
    }

    /**
     * Replaces a file in one step, so an interrupted migration can never leave a configuration
     * that is missing the keys it was about to lose and does not yet name where they went.
     */
    private static void write(Path file, List<String> lines) {
        Path directory = file.toAbsolutePath().getParent();
        if (directory == null) {
            throw new IllegalStateException("Cannot write " + file + ": it has no parent directory.");
        }

        Path staging = file.resolveSibling(file.getFileName() + STAGING_SUFFIX);
        StringBuilder contents = new StringBuilder();
        for (String line : lines) {
            contents.append(line).append('\n');
        }

        try {
            Files.deleteIfExists(staging);
            Files.write(staging, contents.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            try (FileChannel channel = FileChannel.open(staging, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(staging, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            catch (AtomicMoveNotSupportedException exception) {
                Files.move(staging, file, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        catch (IOException exception) {
            try {
                Files.deleteIfExists(staging);
            }
            catch (IOException ignored) {
                // The staging file is named so that a later run replaces it.
            }
            throw new UncheckedIOException("Failed to write " + file, exception);
        }
    }

    private static void createParent(Path file) {
        Path parent = file.getParent();
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

    /** The two halves a configuration file is split into. */
    static final class Split {

        final List<String> configuration;
        final List<String> database;

        Split(List<String> configuration, List<String> database) {
            this.configuration = configuration;
            this.database = database;
        }
    }
}

package net.coreprotect.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LegacyDatabaseConfigurationTest {

    private static final Logger LOG = Logger.getLogger(LegacyDatabaseConfigurationTest.class.getName());

    private static final List<String> LEGACY_CONFIGURATION = Arrays.asList(
            "# CoreProtect Config",
            "",
            "# Database engine used by CoreProtect.",
            "database-type: mysql",
            "",
            "# Connection settings for MySQL.",
            "mysql-host: db.example.test",
            "mysql-port: 3306",
            "mysql-password: hunter2",
            "",
            "# Logs blocks placed by players.",
            "block-place: true");

    @Test
    void movesEveryDatabaseKeyIntoStorage(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"), LEGACY_CONFIGURATION);
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertEquals(Arrays.asList(
                "# CoreProtect Database Config",
                "",
                "# Database engine used by CoreProtect.",
                "database-type: mysql",
                "",
                "# Connection settings for MySQL.",
                "mysql-host: db.example.test",
                "mysql-port: 3306",
                "mysql-password: hunter2"), read(databaseConfiguration));
    }

    @Test
    void leavesEverythingElseInThePluginFolder(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"), LEGACY_CONFIGURATION);

        LegacyDatabaseConfiguration.split(configuration, directory.resolve("storage").resolve("database.yml"), LOG);

        assertEquals(Arrays.asList(
                "# CoreProtect Config",
                "",
                "# Logs blocks placed by players.",
                "block-place: true"), read(configuration));
    }

    @Test
    void carriesTheLegacyMySqlSwitchAcrossSoTheEngineIsNotLost(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"),
                Arrays.asList("# CoreProtect Config", "use-mysql: true", "verbose: true"));
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertTrue(read(databaseConfiguration).contains("use-mysql: true"));
        assertEquals(Arrays.asList("# CoreProtect Config", "verbose: true"), read(configuration));
    }

    @Test
    void doesNothingWhenThereIsNothingToMove(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"),
                Arrays.asList("# CoreProtect Config", "verbose: true"));
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertFalse(Files.exists(databaseConfiguration));
        assertEquals(Arrays.asList("# CoreProtect Config", "verbose: true"), read(configuration));
    }

    @Test
    void doesNothingWhenThereIsNoConfiguration(@TempDir Path directory) {
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(directory.resolve("plugin").resolve("config.yml"), databaseConfiguration, LOG);

        assertFalse(Files.exists(databaseConfiguration));
    }

    @Test
    void runningASecondTimeChangesNothing(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"), LEGACY_CONFIGURATION);
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);
        List<String> afterFirst = read(databaseConfiguration);
        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertEquals(afterFirst, read(databaseConfiguration));
        assertEquals(Arrays.asList(
                "# CoreProtect Config",
                "",
                "# Logs blocks placed by players.",
                "block-place: true"), read(configuration));
    }

    @Test
    void keepsTheStorageCopyAuthoritativeAndSetsTheOtherAside(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"), LEGACY_CONFIGURATION);
        Path databaseConfiguration = write(directory.resolve("storage").resolve("database.yml"),
                Arrays.asList("# CoreProtect Database Config", "database-type: sqlite"));

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertEquals(Arrays.asList("# CoreProtect Database Config", "database-type: sqlite"),
                read(databaseConfiguration));
        assertTrue(read(configuration.resolveSibling("config.yml.database-legacy")).contains("mysql-password: hunter2"));
        assertEquals(Arrays.asList(
                "# CoreProtect Config",
                "",
                "# Logs blocks placed by players.",
                "block-place: true"), read(configuration));
    }

    @Test
    void keepsAnOperatorsOwnCommentsWithTheKeyTheyDescribe(@TempDir Path directory) throws IOException {
        Path configuration = write(directory.resolve("plugin").resolve("config.yml"), Arrays.asList(
                "# CoreProtect Config",
                "# reachable over the office VPN only",
                "mysql-host: db.example.test",
                "# how loud rollbacks are",
                "verbose: true"));
        Path databaseConfiguration = directory.resolve("storage").resolve("database.yml");

        LegacyDatabaseConfiguration.split(configuration, databaseConfiguration, LOG);

        assertEquals(Arrays.asList(
                "# CoreProtect Database Config",
                "# reachable over the office VPN only",
                "mysql-host: db.example.test"), read(databaseConfiguration));
        assertEquals(Arrays.asList(
                "# CoreProtect Config",
                "# how loud rollbacks are",
                "verbose: true"), read(configuration));
    }

    private static Path write(Path file, List<String> lines) throws IOException {
        Files.createDirectories(file.getParent());
        Files.write(file, String.join("\n", lines).concat("\n").getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static List<String> read(Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8);
    }
}

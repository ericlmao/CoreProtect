package net.coreprotect.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.storage.LegacyDatabaseConfiguration;
import net.coreprotect.storage.StorageFiles;

class ConfigDatabaseSettingsTest {

    /**
     * The static check inside {@link Config} refuses a database setting that the storage migration
     * would not move out of the plugin folder, because such a key would be re-added on every boot.
     */
    @Test
    void agreesWithTheMigrationAboutWhichSettingsAreDatabaseSettings() {
        assertDoesNotThrow(() -> Class.forName(Config.class.getName()));
    }

    @Test
    void writesTheDatabaseSettingsAndNothingElseToTheStorageFile(@TempDir Path directory) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("database-type", "duckdb");
        values.put("mysql-password", "");
        File file = directory.resolve(StorageFiles.DATABASE_CONFIGURATION).toFile();

        new Config().addMissingOptions(file, values, LegacyDatabaseConfiguration.FILE_HEADER);

        String written = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        assertTrue(written.startsWith(LegacyDatabaseConfiguration.FILE_HEADER));
        assertTrue(written.contains("database-type: duckdb"));
        assertTrue(written.contains("mysql-password: "));
        assertFalse(written.contains("block-place"));
    }

    @Test
    void leavesSettingsTheFileAlreadyCarriesAlone(@TempDir Path directory) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("database-type", "duckdb");
        File file = directory.resolve(StorageFiles.DATABASE_CONFIGURATION).toFile();
        Files.write(file.toPath(), "# CoreProtect Database Config\ndatabase-type: mysql\n".getBytes(StandardCharsets.UTF_8));

        Config existing = new Config();
        existing.load(Files.newInputStream(file.toPath()));
        existing.addMissingOptions(file, values, LegacyDatabaseConfiguration.FILE_HEADER);

        String written = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        assertTrue(written.contains("database-type: mysql"));
        assertFalse(written.contains("database-type: duckdb"));
    }
}

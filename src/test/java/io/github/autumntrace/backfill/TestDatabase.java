package io.github.autumntrace.backfill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real PostgreSQL for the tests: a Testcontainers postgres:18.7 container started once per test
 * run, or the server in BACKFILL_TEST_DB_URL (with BACKFILL_TEST_DB_USER / _PASSWORD) when set.
 * Every test works in its own schema.
 */
public final class TestDatabase {

    private static Database.Settings settings;
    private static final List<String> created = new ArrayList<>();

    private TestDatabase() {}

    public static synchronized Database.Settings settings() {
        if (settings == null) {
            String url = System.getenv("BACKFILL_TEST_DB_URL");
            if (url != null && !url.isBlank()) {
                settings = new Database.Settings(url, env("BACKFILL_TEST_DB_USER", "postgres"),
                        env("BACKFILL_TEST_DB_PASSWORD", "postgres"));
                // A server that outlives the run: drop the schemas this run made.
                Runtime.getRuntime().addShutdownHook(new Thread(TestDatabase::dropCreated));
            } else {
                PostgreSQLContainer container = new PostgreSQLContainer("postgres:18.7-alpine");
                container.start();
                settings = new Database.Settings(container.getJdbcUrl(), container.getUsername(), container.getPassword());
            }
        }
        return settings;
    }

    /** A fresh, migrated schema with a unique name. */
    public static Database fresh(String prefix) {
        String schema = prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        track(schema);
        return Database.open(settings(), schema);
    }

    /** Remembers a schema to drop at the end of the run. */
    public static synchronized void track(String schema) {
        created.add(schema);
    }

    private static synchronized void dropCreated() {
        try (Connection c = DriverManager.getConnection(settings.url(), settings.user(), settings.password());
                Statement statement = c.createStatement()) {
            for (String schema : created) {
                statement.execute("drop schema if exists " + schema + " cascade");
            }
        } catch (Exception e) {
            System.err.println("could not drop test schemas: " + e.getMessage());
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

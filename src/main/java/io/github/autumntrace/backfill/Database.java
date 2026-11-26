package io.github.autumntrace.backfill;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.regex.Pattern;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * One PostgreSQL schema: a connection pool bound to it, Spring JDBC on top, and its Flyway
 * migrations. Each run (reference, crashed, benchmark) gets its own schema in one database.
 */
public final class Database implements AutoCloseable {

    private static final Pattern SCHEMA = Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    public record Settings(String url, String user, String password) {
        public static Settings fromEnvironment() {
            return new Settings(
                    env("BACKFILL_DB_URL", "jdbc:postgresql://localhost:5432/backfill"),
                    env("BACKFILL_DB_USER", "postgres"),
                    env("BACKFILL_DB_PASSWORD", "postgres"));
        }

        private static String env(String name, String fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : value;
        }
    }

    private final String schema;
    private final HikariDataSource dataSource;
    private final Flyway flyway;
    private final JdbcClient jdbc;
    private final TransactionTemplate transactions;

    private Database(Settings settings, String schema) {
        this.schema = schema;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(settings.url());
        config.setUsername(settings.user());
        config.setPassword(settings.password());
        config.setSchema(schema);
        config.setMaximumPoolSize(6);
        config.setPoolName("backfill-" + schema);
        this.dataSource = new HikariDataSource(config);
        this.flyway = Flyway.configure()
                .dataSource(dataSource)
                .schemas(schema)
                .defaultSchema(schema)
                .createSchemas(true)
                .cleanDisabled(false)
                .load();
        this.jdbc = JdbcClient.create(dataSource);
        this.transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    /** Opens the schema and applies any pending migrations. */
    public static Database open(Settings settings, String schema) {
        if (!SCHEMA.matcher(schema).matches()) {
            throw new IllegalArgumentException("schema names are lower case letters, digits and _: " + schema);
        }
        Database db = new Database(settings, schema);
        db.flyway.migrate();
        return db;
    }

    /** Drops everything in the schema and migrates it again. */
    public void reset() {
        flyway.clean();
        flyway.migrate();
    }

    public String schema() {
        return schema;
    }

    public HikariDataSource dataSource() {
        return dataSource;
    }

    public JdbcClient jdbc() {
        return jdbc;
    }

    public TransactionTemplate transactions() {
        return transactions;
    }

    @Override
    public void close() {
        dataSource.close();
    }
}

package io.github.autumntrace.backfill.importer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * One active writer per import: a PostgreSQL session advisory lock keyed on (schema, import id),
 * held on its own connection for as long as the worker runs. A worker killed with -9 loses its
 * connection, and PostgreSQL drops the lock with it, so a crashed import can be resumed at once.
 */
public final class WriterLock implements AutoCloseable {

    /** Somebody else is writing this import right now. */
    public static final class Busy extends RuntimeException {
        Busy(long importId) {
            super("import " + importId + " already has an active writer; wait for it to finish or stop it");
        }
    }

    private static final String KEY = "hashtext(current_schema()), ?::int";

    private final Connection connection;
    private final long importId;

    private WriterLock(Connection connection, long importId) {
        this.connection = connection;
        this.importId = importId;
    }

    public static WriterLock acquire(DataSource dataSource, long importId) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            if (!call(connection, "select pg_try_advisory_lock(" + KEY + ")", importId)) {
                connection.close();
                throw new Busy(importId);
            }
            return new WriterLock(connection, importId);
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    /** Whether a worker holds the lock now. Takes and releases it if nobody does. */
    public static boolean held(DataSource dataSource, long importId) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            if (call(connection, "select pg_try_advisory_lock(" + KEY + ")", importId)) {
                call(connection, "select pg_advisory_unlock(" + KEY + ")", importId);
                return false;
            }
            return true;
        }
    }

    private static boolean call(Connection connection, String sql, long importId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, Math.toIntExact(importId));
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            // The connection goes back to the pool, so release the session lock explicitly.
            call(connection, "select pg_advisory_unlock(" + KEY + ")", importId);
        } finally {
            connection.close();
        }
    }
}

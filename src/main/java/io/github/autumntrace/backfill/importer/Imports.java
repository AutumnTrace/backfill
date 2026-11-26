package io.github.autumntrace.backfill.importer;

import io.github.autumntrace.backfill.source.SourceFile;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;

/** The imports table, outside the chunk transaction: create a row, read rows back. */
public final class Imports {

    public record Row(
            long id, String sourcePath, String sourceSha256, long sourceBytes, int chunkSize, String state,
            long checkpointOffset, long checkpointLine, int chunksCommitted, long accepted, long duplicates,
            long conflicts, long rejected, OffsetDateTime startedAt, OffsetDateTime updatedAt,
            OffsetDateTime completedAt, String lastWorker) {

        public boolean completed() {
            return "completed".equals(state);
        }
    }

    private final JdbcClient jdbc;

    public Imports(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long create(SourceFile source, int chunkSize) {
        return jdbc.sql("""
                insert into imports (source_path, source_sha256, source_bytes, chunk_size, state)
                values (:path, :sha, :bytes, :chunk, 'running')
                returning id
                """)
                .param("path", source.path().toString()).param("sha", source.sha256())
                .param("bytes", source.bytes()).param("chunk", chunkSize)
                .query(Long.class).single();
    }

    public Optional<Row> find(long id) {
        return jdbc.sql("select * from imports where id = :id").param("id", id).query(Row.class).optional();
    }

    public List<Row> all() {
        return jdbc.sql("select * from imports order by id").query(Row.class).list();
    }

    /** An import of the same bytes that has not finished, if any. */
    public Optional<Row> unfinished(String sha256) {
        return jdbc.sql("select * from imports where source_sha256 = :sha and state = 'running' order by id limit 1")
                .param("sha", sha256).query(Row.class).optional();
    }
}

package io.github.autumntrace.backfill.importer;

import io.github.autumntrace.backfill.Database;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Outcome;
import io.github.autumntrace.backfill.source.LineReader;
import io.github.autumntrace.backfill.source.LineReader.Chunk;
import io.github.autumntrace.backfill.source.SourceFile;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.function.Consumer;

/**
 * Drives an import: checks the source, takes the writer lock, then reads and applies one chunk at a
 * time from the checkpoint until the end of the file.
 */
public final class ImportRunner {

    /** The run cannot start: a changed source file, a finished import, an unfinished one, ... */
    public static final class Refused extends RuntimeException {
        public Refused(String message) {
            super(message);
        }
    }

    public record Progress(long importId, Chunk chunk, Outcome outcome, long sourceBytes) {}

    private final Database db;
    private final Imports imports;
    private final ChunkTransaction chunks;
    private final String worker;

    public ImportRunner(Database db) {
        this.db = db;
        this.imports = new Imports(db.jdbc());
        this.chunks = new ChunkTransaction(db.jdbc(), db.transactions());
        this.worker = "pid " + ProcessHandle.current().pid();
    }

    /** Starts a new import of the file. Refuses if an import of the same bytes is unfinished, unless forced. */
    public long start(Path file, int chunkSize, boolean allowParallel, Crash crash, Consumer<Progress> progress)
            throws IOException, SQLException {
        SourceFile source = SourceFile.inspect(file);
        if (!allowParallel) {
            var unfinished = imports.unfinished(source.sha256());
            if (unfinished.isPresent()) {
                throw new Refused("import " + unfinished.get().id() + " of this file is unfinished; resume it with `resume "
                        + unfinished.get().id() + "`, or pass --new to start another import of the same file");
            }
        }
        long id = imports.create(source, chunkSize);
        run(id, source, crash, progress);
        return id;
    }

    /** Continues an import from its checkpoint, after checking the file is byte for byte the same. */
    public void resume(long id, Path overridePath, Crash crash, Consumer<Progress> progress) throws IOException, SQLException {
        Imports.Row row = imports.find(id).orElseThrow(() -> new Refused("no import " + id));
        if (row.completed()) {
            throw new Refused("import " + id + " is already completed");
        }
        Path path = overridePath != null ? overridePath : Path.of(row.sourcePath());
        SourceFile source;
        try {
            source = SourceFile.inspect(path);
        } catch (IOException e) {
            throw new Refused("cannot read the source of import " + id + " at " + path + ": " + e.getMessage());
        }
        if (!source.sha256().equals(row.sourceSha256()) || source.bytes() != row.sourceBytes()) {
            throw new Refused("the source file changed since import " + id + " started: expected sha256 "
                    + row.sourceSha256().substring(0, 12) + "... (" + row.sourceBytes() + " bytes), found "
                    + source.shortSha() + "... (" + source.bytes() + " bytes). A checkpoint offset only means something"
                    + " in the file it was taken from; start a new import instead.");
        }
        run(id, source, crash, progress);
    }

    private void run(long id, SourceFile source, Crash crash, Consumer<Progress> progress) throws IOException, SQLException {
        try (WriterLock lock = WriterLock.acquire(db.dataSource(), id)) {
            // Read the checkpoint only once the lock is held.
            Imports.Row row = imports.find(id).orElseThrow();
            if (row.completed()) {
                return;
            }
            try (LineReader reader = LineReader.open(source.path(), row.checkpointOffset(), row.checkpointLine())) {
                while (true) {
                    Chunk chunk = reader.readChunk(row.chunkSize());
                    Outcome outcome = chunks.apply(id, chunk, crash, worker);
                    progress.accept(new Progress(id, chunk, outcome, source.bytes()));
                    if (outcome.completed()) {
                        return;
                    }
                }
            }
        }
    }
}

package io.github.autumntrace.backfill;

import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Point;
import io.github.autumntrace.backfill.importer.ImportRunner;
import io.github.autumntrace.backfill.importer.Imports;
import io.github.autumntrace.backfill.inventory.Canonical;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class Fixtures {

    public static final Path DEMO = Path.of("data", "scans-demo.ndjson");

    private Fixtures() {}

    /** Thrown in place of a JVM halt, so a test can crash and carry on in the same process. */
    public static final class SimulatedCrash extends Error {
        SimulatedCrash(Point point) {
            super("simulated crash " + point);
        }
    }

    public static Crash crashAt(Point point, long line) {
        return (at, chunk) -> {
            if (at == point && chunk.startLine() < line && line <= chunk.endLine()) {
                throw new SimulatedCrash(point);
            }
        };
    }

    public static long importAll(Database db, Path file, int chunkSize) throws Exception {
        return new ImportRunner(db).start(file, chunkSize, true, Crash.NONE, p -> {});
    }

    public static List<String> canonical(Database db) {
        return Canonical.of(db.jdbc());
    }

    public static Imports.Row row(Database db, long id) {
        return new Imports(db.jdbc()).find(id).orElseThrow();
    }

    public static Path write(Path dir, String name, String content) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    public static String scan(String id, String warehouse, String sku, int delta, String device, long seq) {
        return "{\"eventId\":\"" + id + "\",\"warehouse\":\"" + warehouse + "\",\"sku\":\"" + sku + "\",\"delta\":" + delta
                + ",\"source\":{\"device\":\"" + device + "\",\"seq\":" + seq + "},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
    }
}

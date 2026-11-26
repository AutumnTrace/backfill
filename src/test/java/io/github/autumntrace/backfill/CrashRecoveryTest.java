package io.github.autumntrace.backfill;

import static io.github.autumntrace.backfill.Fixtures.DEMO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.autumntrace.backfill.Fixtures.SimulatedCrash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Point;
import io.github.autumntrace.backfill.importer.ImportRunner;
import io.github.autumntrace.backfill.importer.Imports;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Crash at a chosen line, just before or just after the COMMIT of the chunk that holds it, then
 * resume. The result must equal an uninterrupted run of the same file, line for line.
 */
class CrashRecoveryTest {

    static Database reference;
    static List<String> expected;

    @BeforeAll
    static void referenceRun() throws Exception {
        reference = TestDatabase.fresh("reference");
        Fixtures.importAll(reference, DEMO, 500);
        expected = Fixtures.canonical(reference);
    }

    @AfterAll
    static void close() {
        reference.close();
    }

    @Test
    void the_reference_run_sees_every_kind_of_record_in_the_demo_file() {
        Imports.Row row = Fixtures.row(reference, 1);
        assertThat(row.accepted()).isEqualTo(11_921);
        assertThat(row.duplicates()).isEqualTo(50);
        assertThat(row.conflicts()).isEqualTo(1);
        assertThat(row.rejected()).isEqualTo(28);
        assertThat(row.checkpointLine()).isEqualTo(12_000);
    }

    @ParameterizedTest(name = "{0} in the chunk holding line {1}")
    @CsvSource({
        "BEFORE_COMMIT, 1",      // the first chunk
        "BEFORE_COMMIT, 6000",   // the middle
        "BEFORE_COMMIT, 12000",  // the final chunk
        "AFTER_COMMIT, 1",
        "AFTER_COMMIT, 6000",
        "AFTER_COMMIT, 12000",
    })
    void a_crash_then_a_resume_produces_the_reference_inventory(Point point, long line) throws Exception {
        try (Database db = TestDatabase.fresh("crash")) {
            assertThatThrownBy(() -> new ImportRunner(db).start(DEMO, 500, false, Fixtures.crashAt(point, line), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);

            // Before the COMMIT the chunk is lost with its checkpoint; after it, both are kept.
            long chunkStart = (line - 1) / 500 * 500;
            Imports.Row interrupted = Fixtures.row(db, 1);
            assertThat(interrupted.checkpointLine()).isEqualTo(point == Point.BEFORE_COMMIT ? chunkStart : chunkStart + 500);
            assertThat(interrupted.completed()).isEqualTo(point == Point.AFTER_COMMIT && line == 12_000);

            if (!interrupted.completed()) {
                new ImportRunner(db).resume(1, null, Crash.NONE, p -> {});
            }
            assertThat(Fixtures.canonical(db)).containsExactlyElementsOf(expected);
            assertThat(Fixtures.row(db, 1).accepted()).isEqualTo(11_921);
        }
    }

    @Test
    void crashing_twice_in_a_row_still_converges() throws Exception {
        try (Database db = TestDatabase.fresh("crash")) {
            assertThatThrownBy(() -> new ImportRunner(db).start(DEMO, 500, false, Fixtures.crashAt(Point.BEFORE_COMMIT, 3000), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            assertThatThrownBy(() -> new ImportRunner(db).resume(1, null, Fixtures.crashAt(Point.AFTER_COMMIT, 9000), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            new ImportRunner(db).resume(1, null, Crash.NONE, p -> {});
            assertThat(Fixtures.canonical(db)).containsExactlyElementsOf(expected);
        }
    }

    @Test
    void chunk_size_does_not_change_the_result() throws Exception {
        for (int chunkSize : new int[] {1, 7, 4096, 100_000}) {
            try (Database db = TestDatabase.fresh("chunks")) {
                Fixtures.importAll(db, DEMO, chunkSize);
                assertThat(Fixtures.canonical(db)).as("chunk size %d", chunkSize).containsExactlyElementsOf(expected);
            }
        }
    }
}

package io.github.autumntrace.backfill;

import static io.github.autumntrace.backfill.Fixtures.DEMO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.autumntrace.backfill.Fixtures.SimulatedCrash;
import io.github.autumntrace.backfill.importer.ChunkTransaction;
import io.github.autumntrace.backfill.importer.ChunkTransaction.CheckpointMoved;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Point;
import io.github.autumntrace.backfill.importer.ImportRunner;
import io.github.autumntrace.backfill.importer.WriterLock;
import io.github.autumntrace.backfill.source.LineReader;
import io.github.autumntrace.backfill.source.LineReader.Chunk;
import io.github.autumntrace.backfill.source.SourceFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Two workers at once never apply a scan twice. */
class TwoWritersTest {

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
    void a_second_worker_on_the_same_import_is_turned_away_while_the_first_holds_the_lock() throws Exception {
        try (Database db = TestDatabase.fresh("writers")) {
            assertThatThrownBy(() -> new ImportRunner(db).start(DEMO, 500, false, Fixtures.crashAt(Point.AFTER_COMMIT, 4000), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            try (WriterLock first = WriterLock.acquire(db.dataSource(), 1)) {
                assertThat(WriterLock.held(db.dataSource(), 1)).isTrue();
                assertThatThrownBy(() -> new ImportRunner(db).resume(1, null, Crash.NONE, p -> {}))
                        .isInstanceOf(WriterLock.Busy.class);
            }
            assertThat(WriterLock.held(db.dataSource(), 1)).isFalse();
            new ImportRunner(db).resume(1, null, Crash.NONE, p -> {});
            assertThat(Fixtures.canonical(db)).containsExactlyElementsOf(expected);
        }
    }

    @Test
    void two_workers_resuming_at_once_from_separate_pools_finish_with_the_reference_result() throws Exception {
        try (Database db = TestDatabase.fresh("writers")) {
            assertThatThrownBy(() -> new ImportRunner(db).start(DEMO, 500, false, Fixtures.crashAt(Point.BEFORE_COMMIT, 4000), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            List<Object> outcomes = race(2, () -> {
                try (Database worker = Database.open(TestDatabase.settings(), db.schema())) {
                    new ImportRunner(worker).resume(1, null, Crash.NONE, p -> {});
                    return "completed";
                } catch (WriterLock.Busy e) {
                    return "busy";
                } catch (ImportRunner.Refused e) {
                    return "already completed"; // the other worker finished before this one looked
                }
            });
            assertThat(outcomes).contains("completed");
            assertThat(Fixtures.canonical(db)).containsExactlyElementsOf(expected);
        }
    }

    // Below the advisory lock: two transactions handed the very same chunk. The row lock on the
    // import queues the second; it then finds the checkpoint moved and rolls back.
    @Test
    void the_same_chunk_applied_twice_at_once_commits_once() throws Exception {
        try (Database db = TestDatabase.fresh("writers")) {
            long id = new io.github.autumntrace.backfill.importer.Imports(db.jdbc()).create(SourceFile.inspect(DEMO), 500);
            Chunk chunk;
            try (LineReader reader = LineReader.open(DEMO, 0, 0)) {
                chunk = reader.readChunk(500);
            }
            ChunkTransaction tx = new ChunkTransaction(db.jdbc(), db.transactions());
            List<Object> outcomes = race(8, () -> {
                try {
                    return tx.apply(id, chunk, Crash.NONE, "test");
                } catch (CheckpointMoved e) {
                    return "moved";
                }
            });
            assertThat(outcomes).filteredOn("moved"::equals).hasSize(7);
            assertThat(db.jdbc().sql("select count(*) from applied_events").query(Long.class).single())
                    .isEqualTo(((ChunkTransaction.Outcome) outcomes.stream().filter(o -> !"moved".equals(o)).findFirst().orElseThrow()).accepted());
            assertThat(Fixtures.row(db, id).chunksCommitted()).isEqualTo(1);
            assertThat(Fixtures.row(db, id).checkpointLine()).isEqualTo(500);
        }
    }

    // Two separate imports of the same file at the same time: every scan lands once, and the
    // inventory matches a single import.
    @Test
    void two_imports_of_one_file_running_together_apply_each_scan_once() throws Exception {
        try (Database db = TestDatabase.fresh("writers")) {
            race(2, () -> {
                try (Database worker = Database.open(TestDatabase.settings(), db.schema())) {
                    return new ImportRunner(worker).start(DEMO, 250, true, Crash.NONE, p -> {});
                }
            });
            List<String> stock = Fixtures.canonical(db).stream().filter(l -> l.chars().filter(c -> c == '\t').count() == 2
                    && l.startsWith("AMS") || l.startsWith("RTM") || l.startsWith("UTR")).toList();
            List<String> expectedStock = expected.stream().filter(l -> l.chars().filter(c -> c == '\t').count() == 2
                    && l.startsWith("AMS") || l.startsWith("RTM") || l.startsWith("UTR")).toList();
            assertThat(stock).isNotEmpty().containsExactlyElementsOf(expectedStock);
            assertThat(db.jdbc().sql("select count(*) from applied_events").query(Long.class).single()).isEqualTo(11_921);
            assertThat(db.jdbc().sql("select sum(accepted) from imports").query(Long.class).single()).isEqualTo(11_921);
        }
    }

    static List<Object> race(int workers, Callable<Object> work) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return work.call();
            }));
        }
        start.countDown();
        List<Object> results = new ArrayList<>();
        for (Future<Object> f : futures) {
            results.add(f.get());
        }
        pool.shutdown();
        return results;
    }
}

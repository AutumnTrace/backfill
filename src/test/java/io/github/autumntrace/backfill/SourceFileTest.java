package io.github.autumntrace.backfill;

import static io.github.autumntrace.backfill.Fixtures.scan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.autumntrace.backfill.Fixtures.SimulatedCrash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Point;
import io.github.autumntrace.backfill.importer.ImportRunner;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The source file under the checkpoint: replaced files, final chunks, line endings. */
class SourceFileTest {

    @TempDir
    Path dir;

    static String lines(int n) {
        List<String> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            out.add(scan(new UUID(7, i).toString(), "AMS-1", String.format("SKU-%05d", 10000 + i % 9), i % 5 + 1, "scanner-01", i));
        }
        return String.join("\n", out);
    }

    Path interrupted(Database db, String content) throws Exception {
        Path file = Fixtures.write(dir, "scans.ndjson", content);
        assertThatThrownBy(() -> new ImportRunner(db).start(file, 10, false, Fixtures.crashAt(Point.AFTER_COMMIT, 15), p -> {}))
                .isInstanceOf(SimulatedCrash.class);
        return file;
    }

    void refusesAfter(Consumer<Path> change, String why) throws Exception {
        try (Database db = TestDatabase.fresh("source")) {
            Path file = interrupted(db, lines(40) + "\n");
            change.accept(file);
            assertThatThrownBy(() -> new ImportRunner(db).resume(1, null, Crash.NONE, p -> {}))
                    .as(why)
                    .isInstanceOf(ImportRunner.Refused.class)
                    .hasMessageContaining("the source file changed since import 1 started");
            assertThat(Fixtures.row(db, 1).checkpointLine()).isEqualTo(20);
        }
    }

    @Test
    void a_resume_against_a_modified_file_is_refused() throws Exception {
        refusesAfter(file -> append(file, lines(1) + "\n"), "appended");
        refusesAfter(file -> rewrite(file, s -> s.replaceFirst("\"delta\":2", "\"delta\":3")), "one byte changed");
        refusesAfter(file -> rewrite(file, s -> s.substring(0, s.length() / 2)), "truncated");
    }

    @Test
    void the_same_bytes_at_another_path_can_be_resumed() throws Exception {
        try (Database db = TestDatabase.fresh("source")) {
            Path file = interrupted(db, lines(40) + "\n");
            Path moved = Files.move(file, dir.resolve("moved.ndjson"), StandardCopyOption.REPLACE_EXISTING);
            assertThatThrownBy(() -> new ImportRunner(db).resume(1, null, Crash.NONE, p -> {}))
                    .isInstanceOf(ImportRunner.Refused.class).hasMessageContaining("cannot read the source");
            new ImportRunner(db).resume(1, moved, Crash.NONE, p -> {});
            assertThat(Fixtures.row(db, 1).accepted()).isEqualTo(40);
        }
    }

    @Test
    void an_interrupted_final_chunk_without_a_trailing_newline_is_applied_once() throws Exception {
        // 25 lines, chunks of 10: the final chunk is lines 21-25 and the file ends without '\n'.
        try (Database db = TestDatabase.fresh("source")) {
            Path file = Fixtures.write(dir, "scans.ndjson", lines(25));
            assertThatThrownBy(() -> new ImportRunner(db).start(file, 10, false, Fixtures.crashAt(Point.BEFORE_COMMIT, 25), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            assertThat(Fixtures.row(db, 1).checkpointLine()).isEqualTo(20);
            assertThat(Fixtures.row(db, 1).completed()).isFalse();

            new ImportRunner(db).resume(1, null, Crash.NONE, p -> {});
            assertThat(Fixtures.row(db, 1).completed()).isTrue();
            assertThat(Fixtures.row(db, 1).checkpointOffset()).isEqualTo(Files.size(file));
            assertThat(Fixtures.row(db, 1).accepted()).isEqualTo(25);
            assertThat(db.jdbc().sql("select count(*) from applied_events where line_no = 25").query(Long.class).single()).isEqualTo(1);
        }
    }

    @Test
    void a_crash_after_the_final_commit_leaves_a_completed_import() throws Exception {
        try (Database db = TestDatabase.fresh("source")) {
            Path file = Fixtures.write(dir, "scans.ndjson", lines(25) + "\n");
            assertThatThrownBy(() -> new ImportRunner(db).start(file, 10, false, Fixtures.crashAt(Point.AFTER_COMMIT, 25), p -> {}))
                    .isInstanceOf(SimulatedCrash.class);
            assertThat(Fixtures.row(db, 1).completed()).isTrue();
            assertThatThrownBy(() -> new ImportRunner(db).resume(1, null, Crash.NONE, p -> {}))
                    .isInstanceOf(ImportRunner.Refused.class).hasMessageContaining("already completed");
        }
    }

    @Test
    void windows_line_endings_give_the_same_result() throws Exception {
        try (Database lf = TestDatabase.fresh("source"); Database crlf = TestDatabase.fresh("source")) {
            Fixtures.importAll(lf, Fixtures.write(dir, "lf.ndjson", lines(30) + "\n"), 7);
            Fixtures.importAll(crlf, Fixtures.write(dir, "crlf.ndjson", lines(30).replace("\n", "\r\n") + "\r\n"), 7);
            assertThat(Fixtures.row(crlf, 1).accepted()).isEqualTo(30);
            assertThat(Fixtures.row(crlf, 1).rejected()).isZero();
            assertThat(Fixtures.canonical(crlf)).isEqualTo(Fixtures.canonical(lf));
        }
    }

    @Test
    void an_empty_file_completes_with_nothing_applied() throws Exception {
        try (Database db = TestDatabase.fresh("source")) {
            long id = Fixtures.importAll(db, Fixtures.write(dir, "empty.ndjson", ""), 10);
            assertThat(Fixtures.row(db, id).completed()).isTrue();
            assertThat(Fixtures.row(db, id).checkpointLine()).isZero();
        }
    }

    @Test
    void starting_a_second_import_while_one_is_unfinished_is_refused() throws Exception {
        try (Database db = TestDatabase.fresh("source")) {
            Path file = interrupted(db, lines(40) + "\n");
            assertThatThrownBy(() -> new ImportRunner(db).start(file, 10, false, Crash.NONE, p -> {}))
                    .isInstanceOf(ImportRunner.Refused.class).hasMessageContaining("resume it with `resume 1`");
        }
    }

    static void append(Path file, String text) {
        try {
            Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void rewrite(Path file, java.util.function.UnaryOperator<String> change) {
        try {
            Files.writeString(file, change.apply(Files.readString(file)), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

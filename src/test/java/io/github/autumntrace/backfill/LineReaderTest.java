package io.github.autumntrace.backfill;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.autumntrace.backfill.dataset.Generator;
import io.github.autumntrace.backfill.source.LineReader;
import io.github.autumntrace.backfill.source.LineReader.Chunk;
import io.github.autumntrace.backfill.source.LineReader.Line;
import io.github.autumntrace.backfill.source.SourceFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Byte offsets and checkpoints, without a database. */
class LineReaderTest {

    @TempDir
    Path dir;

    @Test
    void offsets_are_bytes_and_survive_multibyte_text_and_crlf() throws Exception {
        Path file = dir.resolve("f.ndjson");
        Files.write(file, "é\r\nab\n€€\nlast".getBytes(StandardCharsets.UTF_8));
        try (LineReader reader = LineReader.open(file, 0, 0)) {
            Chunk chunk = reader.readChunk(10);
            assertThat(chunk.lines()).extracting(Line::text).containsExactly("é", "ab", "€€", "last");
            assertThat(chunk.lines()).extracting(Line::offset).containsExactly(0L, 4L, 7L, 14L);
            assertThat(chunk.endOffset()).isEqualTo(18L).isEqualTo(Files.size(file));
            assertThat(chunk.atEnd()).isTrue();
        }
    }

    @Test
    void reading_from_a_checkpoint_gives_the_same_lines_as_reading_through() throws Exception {
        Path file = dir.resolve("f.ndjson");
        Generator.write(file, 1000, 42);
        List<Line> through = new ArrayList<>();
        try (LineReader reader = LineReader.open(file, 0, 0)) {
            through.addAll(reader.readChunk(10_000).lines());
        }
        List<Line> resumed = new ArrayList<>();
        try (LineReader reader = LineReader.open(file, 0, 0)) {
            Chunk first = reader.readChunk(377);
            resumed.addAll(first.lines());
            try (LineReader rest = LineReader.open(file, first.endOffset(), first.endLine())) {
                Chunk chunk;
                do {
                    chunk = rest.readChunk(100);
                    resumed.addAll(chunk.lines());
                } while (!chunk.atEnd());
            }
        }
        assertThat(resumed).isEqualTo(through);
    }

    @Test
    void a_long_line_is_cut_but_the_next_one_starts_in_the_right_place() throws Exception {
        Path file = dir.resolve("f.ndjson");
        Files.writeString(file, "x".repeat(10_000) + "\nnext\n");
        try (LineReader reader = LineReader.open(file, 0, 0)) {
            List<Line> lines = reader.readChunk(10).lines();
            assertThat(lines.get(0).truncated()).isTrue();
            assertThat(lines.get(0).text()).hasSize(LineReader.MAX_LINE_BYTES);
            assertThat(lines.get(1).offset()).isEqualTo(10_001);
            assertThat(lines.get(1).text()).isEqualTo("next");
        }
    }

    @Test
    void the_generator_is_deterministic_per_seed() throws Exception {
        Generator.Summary a = Generator.write(dir.resolve("a.ndjson"), 5000, 99);
        Generator.write(dir.resolve("b.ndjson"), 5000, 99);
        Generator.write(dir.resolve("c.ndjson"), 5000, 100);
        assertThat(SourceFile.inspect(dir.resolve("a.ndjson")).sha256()).isEqualTo(SourceFile.inspect(dir.resolve("b.ndjson")).sha256());
        assertThat(SourceFile.inspect(dir.resolve("a.ndjson")).sha256()).isNotEqualTo(SourceFile.inspect(dir.resolve("c.ndjson")).sha256());
        assertThat(a.conflictEventId()).isNotNull();
        assertThat(a.repeats()).isPositive();
        assertThat(a.malformed()).isPositive();
    }

    @Test
    void the_committed_demo_file_is_the_seeded_one() throws Exception {
        Path regenerated = dir.resolve("demo.ndjson");
        Generator.write(regenerated, 12_000, 20261126);
        assertThat(SourceFile.inspect(regenerated).sha256()).isEqualTo(SourceFile.inspect(Fixtures.DEMO).sha256());
    }
}

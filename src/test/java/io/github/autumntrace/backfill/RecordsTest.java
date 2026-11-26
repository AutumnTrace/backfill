package io.github.autumntrace.backfill;

import static io.github.autumntrace.backfill.Fixtures.scan;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.autumntrace.backfill.importer.Imports;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Duplicates, conflicts and malformed lines, on small hand-written files. */
class RecordsTest {

    static final String A = "00000000-0000-4000-8000-00000000000a";
    static final String B = "00000000-0000-4000-8000-00000000000b";

    @TempDir
    Path dir;

    @ParameterizedTest(name = "chunk size {0}")
    @ValueSource(ints = {1, 2, 100})
    void identical_duplicates_do_nothing_and_a_conflict_is_quarantined_with_both_payloads(int chunkSize) throws Exception {
        String a = scan(A, "AMS-1", "SKU-10001", 5, "scanner-01", 1);
        // The same scan with other key order and spacing: same fields, so the same scan.
        String aAgain = "{ \"sku\": \"SKU-10001\", \"warehouse\": \"AMS-1\", \"eventId\": \"" + A
                + "\", \"delta\": 5, \"scannedAt\": \"2026-10-01T06:00:00Z\", \"source\": {\"seq\": 1, \"device\": \"scanner-01\"} }";
        String b = scan(B, "AMS-1", "SKU-10002", -2, "scanner-01", 2);
        String aConflict = scan(A, "AMS-1", "SKU-10001", 50, "scanner-01", 1);
        Path file = Fixtures.write(dir, "scans.ndjson", String.join("\n", a, aAgain, b, aConflict, a) + "\n");

        try (Database db = TestDatabase.fresh("records")) {
            long id = Fixtures.importAll(db, file, chunkSize);
            Imports.Row row = Fixtures.row(db, id);
            assertThat(row.accepted()).isEqualTo(2);
            assertThat(row.duplicates()).isEqualTo(2);
            assertThat(row.conflicts()).isEqualTo(1);
            assertThat(stock(db)).containsExactlyInAnyOrderEntriesOf(Map.of("SKU-10001", 5L, "SKU-10002", -2L));

            Map<String, Object> q = db.jdbc().sql("select * from quarantine").query().singleRow();
            assertThat(q.get("event_id").toString()).isEqualTo(A);
            assertThat(q.get("line_no")).isEqualTo(4L);
            assertThat(q.get("payload")).isEqualTo(aConflict);
            // The first payload is reachable by position: line 1, byte 0, in import 1.
            assertThat(q.get("first_line_no")).isEqualTo(1L);
            assertThat(q.get("first_byte_offset")).isEqualTo(0L);
            assertThat(q.get("first_import_id")).isEqualTo(id);
            assertThat(q.get("first_payload_sha256")).isNotEqualTo(q.get("payload_sha256"));
        }
    }

    @Test
    void a_second_import_of_the_same_file_changes_nothing() throws Exception {
        String content = String.join("\n", scan(A, "AMS-1", "SKU-10001", 5, "scanner-01", 1),
                scan(B, "RTM-2", "SKU-10002", 3, "scanner-02", 1)) + "\n";
        Path file = Fixtures.write(dir, "scans.ndjson", content);
        try (Database db = TestDatabase.fresh("records")) {
            Fixtures.importAll(db, file, 10);
            List<String> once = Fixtures.canonical(db).stream().filter(l -> !l.startsWith("duplicates")).toList();
            long second = Fixtures.importAll(db, file, 10);
            assertThat(Fixtures.row(db, second).accepted()).isZero();
            assertThat(Fixtures.row(db, second).duplicates()).isEqualTo(2);
            assertThat(Fixtures.canonical(db).stream().filter(l -> !l.startsWith("duplicates")).toList()).isEqualTo(once);
        }
    }

    @Test
    void malformed_lines_are_rejected_with_a_reason_and_their_position() throws Exception {
        String good = scan(B, "UTR-3", "SKU-10003", 1, "scanner-03", 1);
        List<String> lines = List.of(
                good,
                "{\"eventId\":\"" + A + "\",\"warehouse\":",                                           // 2
                scan(A, "UTR-3", "SKU-10003", 1, "scanner-03", 2).replace(",\"sku\":\"SKU-10003\"", ""), // 3
                scan(A, "UTR-3", "SKU-10003", 0, "scanner-03", 2),                                       // 4
                scan(A, "UTR-3", "SKU-10003", 20_000, "scanner-03", 2),                                  // 5
                scan("not-a-uuid", "UTR-3", "SKU-10003", 1, "scanner-03", 2),                            // 6
                scan(A, "utr3", "SKU-10003", 1, "scanner-03", 2),                                        // 7
                scan(A, "UTR-3", "PEN-1", 1, "scanner-03", 2),                                           // 8
                scan(A, "UTR-3", "SKU-10003", 1, "scanner-03", 2).replace("\"seq\":2", "\"seq\":-1"),    // 9
                scan(A, "UTR-3", "SKU-10003", 1, "scanner-03", 2).replace("2026-10-01T06:00:00Z", "soon"), // 10
                "",                                                                                       // 11
                "[1,2,3]",                                                                                // 12
                "x".repeat(9000),                                                                         // 13
                scan(A, "UTR-3", "SKU-10003", 2, "scanner-03", 2));                                       // 14
        Path file = Fixtures.write(dir, "mixed.ndjson", String.join("\n", lines) + "\n");

        try (Database db = TestDatabase.fresh("records")) {
            long id = Fixtures.importAll(db, file, 4);
            Map<Long, String> reasons = new java.util.TreeMap<>();
            db.jdbc().sql("select line_no, reason from rejects").query((rs, i) -> reasons.put(rs.getLong(1), rs.getString(2))).list();
            assertThat(reasons).containsExactlyEntriesOf(new java.util.TreeMap<>(Map.ofEntries(
                    Map.entry(2L, "malformed_json"),
                    Map.entry(3L, "missing_field:sku"),
                    Map.entry(4L, "invalid_delta"),
                    Map.entry(5L, "invalid_delta"),
                    Map.entry(6L, "invalid_event_id"),
                    Map.entry(7L, "invalid_warehouse"),
                    Map.entry(8L, "invalid_sku"),
                    Map.entry(9L, "invalid_source"),
                    Map.entry(10L, "invalid_scanned_at"),
                    Map.entry(11L, "blank_line"),
                    Map.entry(12L, "malformed_json"),
                    Map.entry(13L, "line_too_long"))));

            // Offsets are bytes: line 14 starts after every byte of lines 1 to 13 and their newlines.
            long expectedOffset = lines.subList(0, 13).stream().mapToLong(l -> l.getBytes(StandardCharsets.UTF_8).length + 1).sum();
            assertThat(db.jdbc().sql("select byte_offset from applied_events where line_no = 14").query(Long.class).single())
                    .isEqualTo(expectedOffset);
            Imports.Row row = Fixtures.row(db, id);
            assertThat(row.rejected()).isEqualTo(12);
            assertThat(row.accepted()).isEqualTo(2);
            assertThat(row.checkpointLine()).isEqualTo(14);
            assertThat(stock(db)).containsExactlyEntriesOf(Map.of("SKU-10003", 3L));
        }
    }

    static Map<String, Long> stock(Database db) {
        Map<String, Long> stock = new java.util.HashMap<>();
        db.jdbc().sql("select sku, quantity from stock").query((rs, i) -> stock.put(rs.getString(1), rs.getLong(2))).list();
        return stock;
    }
}

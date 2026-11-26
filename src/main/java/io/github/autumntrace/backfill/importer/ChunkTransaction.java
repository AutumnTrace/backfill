package io.github.autumntrace.backfill.importer;

import io.github.autumntrace.backfill.importer.ScanParser.Invalid;
import io.github.autumntrace.backfill.importer.ScanParser.Parsed;
import io.github.autumntrace.backfill.importer.ScanParser.Scan;
import io.github.autumntrace.backfill.importer.ScanParser.Valid;
import io.github.autumntrace.backfill.source.LineReader.Chunk;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * One chunk of the source file, applied in one database transaction. This is the whole of it.
 *
 * <pre>
 *   BEGIN
 *   1. lock the import row; its checkpoint must equal this chunk's start offset
 *   2. insert the chunk's scans into applied_events ON CONFLICT (event_id) DO NOTHING
 *      - inserted: accepted
 *      - already there with the same payload hash: duplicate, no effect
 *      - already there with another payload hash: conflict, quarantined with both references
 *   3. stock += sum of accepted deltas per (warehouse, sku), keys in sorted order
 *   4. insert rejects and quarantine rows, keyed by (import, line)
 *   5. advance the checkpoint and every counter (and mark the import completed on the last chunk)
 *   COMMIT
 * </pre>
 *
 * Either all of that is visible or none of it is. A crash before COMMIT loses the chunk and the
 * checkpoint together, so a resume redoes exactly that chunk. A crash after COMMIT keeps both, so
 * a resume starts after it. In neither case can a scan be applied twice: step 2 is keyed on the
 * event id, and step 1 stops a second writer that read the same checkpoint.
 */
public final class ChunkTransaction {

    public enum Point { BEFORE_COMMIT, AFTER_COMMIT }

    /** Called at both points; the crash demo and the tests use it to stop at an exact place. */
    public interface Crash {
        void at(Point point, Chunk chunk);

        Crash NONE = (point, chunk) -> {};
    }

    public record Outcome(int lines, int accepted, int duplicates, int conflicts, int rejected, boolean completed) {}

    /** Another writer moved the checkpoint, or the import was completed under us. */
    public static final class CheckpointMoved extends RuntimeException {
        CheckpointMoved(long importId, long expected, Long found) {
            super("import " + importId + ": expected checkpoint at byte " + expected + " but found "
                    + (found == null ? "the import completed or missing" : "byte " + found)
                    + "; another writer has processed this chunk");
        }
    }

    private record Applied(UUID eventId, String payloadSha256, long importId, long lineNo, long byteOffset) {}

    private record Conflict(Valid scan, Applied first) {}

    private final JdbcClient jdbc;
    private final TransactionTemplate transactions;

    public ChunkTransaction(JdbcClient jdbc, TransactionTemplate transactions) {
        this.jdbc = jdbc;
        this.transactions = transactions;
    }

    public Outcome apply(long importId, Chunk chunk, Crash crash, String worker) {
        List<Valid> scans = new ArrayList<>();
        List<Invalid> rejects = new ArrayList<>();
        for (var line : chunk.lines()) {
            Parsed parsed = ScanParser.parse(line);
            if (parsed instanceof Valid v) {
                scans.add(v);
            } else {
                rejects.add((Invalid) parsed);
            }
        }

        Outcome outcome = transactions.execute(status -> {
            // 1. One writer per checkpoint. The row lock queues a second writer behind this one;
            //    when it gets the row, the checkpoint has moved and it stops.
            Long checkpoint = jdbc.sql("""
                    select checkpoint_offset from imports where id = :id and state = 'running' for update
                    """)
                    .param("id", importId)
                    .query(Long.class).optional().orElse(null);
            if (checkpoint == null || checkpoint != chunk.startOffset()) {
                throw new CheckpointMoved(importId, chunk.startOffset(), checkpoint);
            }

            // 2. First occurrence of each event id in this chunk goes to the insert; repeats inside
            //    the chunk are judged against whichever row ends up in applied_events.
            Map<UUID, Valid> first = new LinkedHashMap<>();
            List<Valid> repeats = new ArrayList<>();
            for (Valid v : scans) {
                if (first.putIfAbsent(v.scan().eventId(), v) != null) {
                    repeats.add(v);
                }
            }
            Set<UUID> inserted = new HashSet<>(insertApplied(importId, first.values()));
            Map<UUID, Applied> existing = new HashMap<>();
            for (Valid v : first.values()) {
                if (inserted.contains(v.scan().eventId())) {
                    existing.put(v.scan().eventId(), new Applied(v.scan().eventId(), v.scan().payloadSha256(), importId,
                            v.line().lineNo(), v.line().offset()));
                }
            }
            existing.putAll(appliedBefore(first.keySet().stream().filter(id -> !inserted.contains(id)).toList()));

            List<Valid> accepted = new ArrayList<>();
            List<Conflict> conflicts = new ArrayList<>();
            int duplicates = 0;
            for (Valid v : first.values()) {
                if (inserted.contains(v.scan().eventId())) {
                    accepted.add(v);
                } else if (sameAs(v, existing)) {
                    duplicates++;
                } else {
                    conflicts.add(new Conflict(v, existing.get(v.scan().eventId())));
                }
            }
            for (Valid v : repeats) {
                if (sameAs(v, existing)) {
                    duplicates++;
                } else {
                    conflicts.add(new Conflict(v, existing.get(v.scan().eventId())));
                }
            }

            // 3. Stock. Sorted keys, so two writers touching the same rows lock them in one order.
            Map<String, Long> deltas = new TreeMap<>();
            for (Valid v : accepted) {
                deltas.merge(v.scan().warehouse() + "\t" + v.scan().sku(), (long) v.scan().delta(), Long::sum);
            }
            addStock(deltas);

            // 4. Rejects and quarantine.
            insertRejects(importId, rejects);
            insertQuarantine(importId, conflicts);

            // 5. Checkpoint and counters, in the same transaction as everything they count.
            int updated = jdbc.sql("""
                    update imports
                       set checkpoint_offset = :end, checkpoint_line = :endLine,
                           chunks_committed = chunks_committed + 1,
                           accepted = accepted + :accepted, duplicates = duplicates + :duplicates,
                           conflicts = conflicts + :conflicts, rejected = rejected + :rejected,
                           updated_at = now(), last_worker = :worker,
                           state = case when :last then 'completed' else state end,
                           completed_at = case when :last then now() end
                     where id = :id and checkpoint_offset = :start
                    """)
                    .param("end", chunk.endOffset()).param("endLine", chunk.endLine())
                    .param("accepted", accepted.size()).param("duplicates", duplicates)
                    .param("conflicts", conflicts.size()).param("rejected", rejects.size())
                    .param("worker", worker).param("last", chunk.atEnd())
                    .param("id", importId).param("start", chunk.startOffset())
                    .update();
            if (updated != 1) {
                throw new CheckpointMoved(importId, chunk.startOffset(), null);
            }

            crash.at(Point.BEFORE_COMMIT, chunk);
            return new Outcome(chunk.lines().size(), accepted.size(), duplicates, conflicts.size(), rejects.size(),
                    chunk.atEnd());
        });

        crash.at(Point.AFTER_COMMIT, chunk);
        return outcome;
    }

    private static boolean sameAs(Valid v, Map<UUID, Applied> existing) {
        Applied applied = existing.get(v.scan().eventId());
        if (applied == null) {
            throw new IllegalStateException("event " + v.scan().eventId() + " was neither inserted nor found");
        }
        return applied.payloadSha256().equals(v.scan().payloadSha256());
    }

    private List<UUID> insertApplied(long importId, java.util.Collection<Valid> scans) {
        if (scans.isEmpty()) {
            return List.of();
        }
        int n = scans.size();
        UUID[] ids = new UUID[n];
        String[] hashes = new String[n];
        String[] warehouses = new String[n];
        String[] skus = new String[n];
        int[] deltas = new int[n];
        String[] devices = new String[n];
        long[] seqs = new long[n];
        long[] lines = new long[n];
        long[] offsets = new long[n];
        int i = 0;
        for (Valid v : scans) {
            Scan s = v.scan();
            ids[i] = s.eventId();
            hashes[i] = s.payloadSha256();
            warehouses[i] = s.warehouse();
            skus[i] = s.sku();
            deltas[i] = s.delta();
            devices[i] = s.device();
            seqs[i] = s.seq();
            lines[i] = v.line().lineNo();
            offsets[i] = v.line().offset();
            i++;
        }
        return jdbc.sql("""
                insert into applied_events
                       (event_id, payload_sha256, warehouse, sku, delta, source_device, source_seq, import_id, line_no, byte_offset)
                select e.id, e.hash, e.warehouse, e.sku, e.delta, e.device, e.seq, :import, e.line, e.byte_offset
                  from unnest(:ids::uuid[], :hashes::text[], :warehouses::text[], :skus::text[], :deltas::int[],
                              :devices::text[], :seqs::bigint[], :lines::bigint[], :offsets::bigint[])
                       as e(id, hash, warehouse, sku, delta, device, seq, line, byte_offset)
                on conflict (event_id) do nothing
                returning event_id
                """)
                .param("import", importId).param("ids", ids).param("hashes", hashes).param("warehouses", warehouses)
                .param("skus", skus).param("deltas", deltas).param("devices", devices).param("seqs", seqs)
                .param("lines", lines).param("offsets", offsets)
                .query(UUID.class).list();
    }

    private Map<UUID, Applied> appliedBefore(List<UUID> ids) {
        if (ids.isEmpty()) {
            return Map.of();
        }
        Map<UUID, Applied> found = new HashMap<>();
        jdbc.sql("""
                select event_id, payload_sha256, import_id, line_no, byte_offset
                  from applied_events where event_id = any(:ids::uuid[])
                """)
                .param("ids", ids.toArray(UUID[]::new))
                .query((rs, row) -> new Applied(rs.getObject(1, UUID.class), rs.getString(2), rs.getLong(3), rs.getLong(4),
                        rs.getLong(5)))
                .list()
                .forEach(a -> found.put(a.eventId(), a));
        return found;
    }

    private void addStock(Map<String, Long> deltas) {
        if (deltas.isEmpty()) {
            return;
        }
        String[] warehouses = new String[deltas.size()];
        String[] skus = new String[deltas.size()];
        long[] amounts = new long[deltas.size()];
        int i = 0;
        for (var e : deltas.entrySet()) {
            String[] key = e.getKey().split("\t");
            warehouses[i] = key[0];
            skus[i] = key[1];
            amounts[i] = e.getValue();
            i++;
        }
        jdbc.sql("""
                insert into stock (warehouse, sku, quantity)
                select * from unnest(:warehouses::text[], :skus::text[], :amounts::bigint[])
                on conflict (warehouse, sku) do update set quantity = stock.quantity + excluded.quantity
                """)
                .param("warehouses", warehouses).param("skus", skus).param("amounts", amounts)
                .update();
    }

    private void insertRejects(long importId, List<Invalid> rejects) {
        for (Invalid r : rejects) {
            jdbc.sql("""
                    insert into rejects (import_id, line_no, byte_offset, reason, raw)
                    values (:import, :line, :offset, :reason, :raw)
                    on conflict do nothing
                    """)
                    .param("import", importId).param("line", r.line().lineNo()).param("offset", r.line().offset())
                    .param("reason", r.reason()).param("raw", truncate(r.line().text()))
                    .update();
        }
    }

    private void insertQuarantine(long importId, List<Conflict> conflicts) {
        for (Conflict c : conflicts) {
            jdbc.sql("""
                    insert into quarantine (import_id, line_no, byte_offset, event_id, payload_sha256, payload,
                                            first_import_id, first_line_no, first_byte_offset, first_payload_sha256)
                    values (:import, :line, :offset, :event, :hash, :payload, :firstImport, :firstLine, :firstOffset, :firstHash)
                    on conflict do nothing
                    """)
                    .param("import", importId).param("line", c.scan().line().lineNo())
                    .param("offset", c.scan().line().offset()).param("event", c.scan().scan().eventId())
                    .param("hash", c.scan().scan().payloadSha256()).param("payload", c.scan().line().text())
                    .param("firstImport", c.first().importId()).param("firstLine", c.first().lineNo())
                    .param("firstOffset", c.first().byteOffset()).param("firstHash", c.first().payloadSha256())
                    .update();
        }
    }

    private static String truncate(String raw) {
        return raw.length() > 1000 ? raw.substring(0, 1000) + "…" : raw;
    }
}

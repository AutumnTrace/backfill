package io.github.autumntrace.backfill.inventory;

import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The result of a run as sorted text, so two runs can be compared line by line: the inventory,
 * the event counts, every quarantined conflict and every reject. Ids, timestamps and chunk counts
 * are left out; they differ between runs that reached the same state.
 */
public final class Canonical {

    private Canonical() {}

    public static List<String> of(JdbcClient jdbc) {
        List<String> out = new ArrayList<>();
        out.add("# backfill canonical output v1");
        out.add("events applied\t" + count(jdbc, "select count(*) from applied_events"));
        out.add("duplicates\t" + count(jdbc, "select coalesce(sum(duplicates), 0) from imports"));
        out.add("conflicts\t" + count(jdbc, "select count(*) from quarantine"));
        out.add("rejected\t" + count(jdbc, "select count(*) from rejects"));
        out.add("stock rows\t" + count(jdbc, "select count(*) from stock"));
        out.add("stock total\t" + count(jdbc, "select coalesce(sum(quantity), 0) from stock"));
        out.add("## stock: warehouse, sku, quantity");
        out.addAll(jdbc.sql("select warehouse || E'\\t' || sku || E'\\t' || quantity from stock order by warehouse, sku")
                .query(String.class).list());
        out.add("## quarantine: event id, line, first line, payload hash, first payload hash");
        out.addAll(jdbc.sql("""
                select event_id || E'\\t' || line_no || E'\\t' || first_line_no || E'\\t' || payload_sha256 || E'\\t' || first_payload_sha256
                  from quarantine order by line_no, event_id
                """).query(String.class).list());
        out.add("## rejects: line, byte offset, reason");
        out.addAll(jdbc.sql("select line_no || E'\\t' || byte_offset || E'\\t' || reason from rejects order by line_no")
                .query(String.class).list());
        return out;
    }

    /** The first differing line, or null when the two are the same. */
    public static String firstDifference(List<String> a, List<String> b) {
        for (int i = 0; i < Math.max(a.size(), b.size()); i++) {
            String left = i < a.size() ? a.get(i) : "<end>";
            String right = i < b.size() ? b.get(i) : "<end>";
            if (!left.equals(right)) {
                return "line " + (i + 1) + ":\n  " + left + "\n  " + right;
            }
        }
        return null;
    }

    private static long count(JdbcClient jdbc, String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }
}

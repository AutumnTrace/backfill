package io.github.autumntrace.backfill.importer;

import io.github.autumntrace.backfill.source.LineReader.Line;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.UUID;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns one line into a {@link Scan} or a reject reason.
 *
 * <pre>
 * {"eventId":"8f0c…","warehouse":"AMS-1","sku":"SKU-10423","delta":-2,
 *  "source":{"device":"scanner-07","seq":18342},"scannedAt":"2026-10-01T08:15:22.123Z"}
 * </pre>
 *
 * The payload hash covers the parsed fields, not the raw bytes: the same scan written with other
 * whitespace or key order is the same scan. Two lines with one event id and different hashes are a
 * conflict.
 */
public final class ScanParser {

    public record Scan(
            UUID eventId, String warehouse, String sku, int delta, String device, long seq, Instant scannedAt,
            String payloadSha256) {}

    public sealed interface Parsed permits Valid, Invalid {}

    public record Valid(Line line, Scan scan) implements Parsed {}

    public record Invalid(Line line, String reason) implements Parsed {}

    public static final int MAX_ABS_DELTA = 10_000;

    private static final Pattern WAREHOUSE = Pattern.compile("[A-Z]{3}-[0-9]{1,2}");
    private static final Pattern SKU = Pattern.compile("SKU-[0-9]{5}");
    private static final Pattern UUID_TEXT = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final ObjectMapper JSON = JsonMapper.builder().build();

    private ScanParser() {}

    public static Parsed parse(Line line) {
        if (line.truncated()) {
            return new Invalid(line, "line_too_long");
        }
        if (line.text().isBlank()) {
            return new Invalid(line, "blank_line");
        }
        JsonNode node;
        try {
            node = JSON.readTree(line.text());
        } catch (JacksonException e) {
            return new Invalid(line, "malformed_json");
        }
        if (node == null || !node.isObject()) {
            return new Invalid(line, "malformed_json");
        }

        for (String field : new String[] {"eventId", "warehouse", "sku", "delta", "source", "scannedAt"}) {
            if (!node.has(field) || node.get(field).isNull()) {
                return new Invalid(line, "missing_field:" + field);
            }
        }
        String eventId = node.get("eventId").asString("");
        if (!node.get("eventId").isString() || !UUID_TEXT.matcher(eventId).matches()) {
            return new Invalid(line, "invalid_event_id");
        }
        String warehouse = node.get("warehouse").asString("");
        if (!node.get("warehouse").isString() || !WAREHOUSE.matcher(warehouse).matches()) {
            return new Invalid(line, "invalid_warehouse");
        }
        String sku = node.get("sku").asString("");
        if (!node.get("sku").isString() || !SKU.matcher(sku).matches()) {
            return new Invalid(line, "invalid_sku");
        }
        JsonNode delta = node.get("delta");
        if (!delta.isIntegralNumber() || !delta.canConvertToInt()
                || delta.asInt() == 0 || Math.abs(delta.asInt()) > MAX_ABS_DELTA) {
            return new Invalid(line, "invalid_delta");
        }
        JsonNode source = node.get("source");
        if (!source.isObject() || !source.path("device").isString() || source.path("device").asString().isBlank()
                || !source.path("seq").isIntegralNumber() || source.path("seq").asLong() < 0) {
            return new Invalid(line, "invalid_source");
        }
        Instant scannedAt;
        try {
            scannedAt = Instant.parse(node.get("scannedAt").asString(""));
        } catch (DateTimeParseException e) {
            return new Invalid(line, "invalid_scanned_at");
        }

        UUID id = UUID.fromString(eventId);
        String device = source.get("device").asString();
        long seq = source.get("seq").asLong();
        String canonical = String.join("|", id.toString(), warehouse, sku, Integer.toString(delta.asInt()), device,
                Long.toString(seq), scannedAt.toString());
        return new Valid(line, new Scan(id, warehouse, sku, delta.asInt(), device, seq, scannedAt, sha256(canonical)));
    }

    static String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

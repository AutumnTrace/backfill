package io.github.autumntrace.backfill.dataset;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * A synthetic, seeded scan log: the same seed and size give the same bytes on every machine.
 *
 * <ul>
 *   <li>three warehouses, 200 SKUs, twelve scanners with their own sequence numbers;
 *   <li>about 0.5% of lines repeat an earlier scan exactly (a scanner resending);
 *   <li>about 0.25% of lines are malformed in one of eight ways;
 *   <li>exactly one event id is reused, at about 70% of the file, with a different delta.
 * </ul>
 */
public final class Generator {

    public record Summary(int lines, int scans, int repeats, int malformed, String conflictEventId, int conflictLine) {}

    private static final String[] WAREHOUSES = {"AMS-1", "RTM-2", "UTR-3"};

    private Generator() {}

    public static Summary write(Path out, int lines, long seed) throws IOException {
        SplittableRandom random = new SplittableRandom(seed);
        long[] deviceSeq = new long[12];
        Instant clock = Instant.parse("2026-10-01T06:00:00Z");
        List<String> recent = new ArrayList<>();
        int scans = 0;
        int repeats = 0;
        int malformed = 0;
        int conflictAt = (int) (lines * 0.7);
        int sourceOfConflict = (int) (lines * 0.2);
        String conflictId = null;
        String[] conflictSource = null;

        Files.createDirectories(out.toAbsolutePath().getParent());
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            for (int line = 1; line <= lines; line++) {
                String text;
                double roll = random.nextDouble();
                if (line == conflictAt && conflictSource != null) {
                    // Same event id, same everything, different delta.
                    int delta = Integer.parseInt(conflictSource[3]) + 5;
                    text = json(conflictSource[0], conflictSource[1], conflictSource[2], delta, conflictSource[4],
                            Long.parseLong(conflictSource[5]), conflictSource[6]);
                    conflictId = conflictSource[0];
                } else if (roll < 0.005 && !recent.isEmpty()) {
                    text = recent.get(random.nextInt(recent.size()));
                    repeats++;
                } else if (roll < 0.0075) {
                    text = malformedLine(random);
                    malformed++;
                } else {
                    String id = new UUID(random.nextLong(), random.nextLong()).toString();
                    String warehouse = WAREHOUSES[random.nextInt(WAREHOUSES.length)];
                    String sku = "SKU-" + (10000 + random.nextInt(200));
                    int delta = random.nextDouble() < 0.3 ? 1 + random.nextInt(24) : -(1 + random.nextInt(6));
                    int device = random.nextInt(deviceSeq.length);
                    long seq = ++deviceSeq[device];
                    clock = clock.plusMillis(1 + random.nextInt(900));
                    String deviceName = String.format("scanner-%02d", device + 1);
                    text = json(id, warehouse, sku, delta, deviceName, seq, clock.toString());
                    scans++;
                    if (line == sourceOfConflict || (conflictSource == null && line > sourceOfConflict)) {
                        conflictSource = new String[] {id, warehouse, sku, Integer.toString(delta), deviceName,
                                Long.toString(seq), clock.toString()};
                    }
                    recent.add(text);
                    if (recent.size() > 50) {
                        recent.remove(0);
                    }
                }
                w.write(text);
                w.write('\n');
            }
        }
        return new Summary(lines, scans, repeats, malformed, conflictId, conflictAt);
    }

    private static String json(String id, String warehouse, String sku, int delta, String device, long seq, String at) {
        return "{\"eventId\":\"" + id + "\",\"warehouse\":\"" + warehouse + "\",\"sku\":\"" + sku + "\",\"delta\":" + delta
                + ",\"source\":{\"device\":\"" + device + "\",\"seq\":" + seq + "},\"scannedAt\":\"" + at + "\"}";
    }

    private static String malformedLine(SplittableRandom random) {
        String id = new UUID(random.nextLong(), random.nextLong()).toString();
        return switch (random.nextInt(8)) {
            case 0 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"AMS-1\",\"sku\":\"SKU-10007\",\"delta\":";
            case 1 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"AMS-1\",\"delta\":3,\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
            case 2 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"AMS-1\",\"sku\":\"SKU-10007\",\"delta\":\"two\",\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
            case 3 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"AMS-1\",\"sku\":\"SKU-10007\",\"delta\":0,\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
            case 4 -> "{\"eventId\":\"scan-0042\",\"warehouse\":\"AMS-1\",\"sku\":\"SKU-10007\",\"delta\":1,\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
            case 5 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"ams1\",\"sku\":\"SKU-10007\",\"delta\":1,\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"2026-10-01T06:00:00Z\"}";
            case 6 -> "{\"eventId\":\"" + id + "\",\"warehouse\":\"AMS-1\",\"sku\":\"SKU-10007\",\"delta\":2,\"source\":{\"device\":\"scanner-02\",\"seq\":1},\"scannedAt\":\"yesterday\"}";
            default -> "";
        };
    }
}

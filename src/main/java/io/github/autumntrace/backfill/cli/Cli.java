package io.github.autumntrace.backfill.cli;

import io.github.autumntrace.backfill.Database;
import io.github.autumntrace.backfill.dataset.Generator;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Crash;
import io.github.autumntrace.backfill.importer.ChunkTransaction.CheckpointMoved;
import io.github.autumntrace.backfill.importer.ChunkTransaction.Point;
import io.github.autumntrace.backfill.importer.ImportRunner;
import io.github.autumntrace.backfill.importer.ImportRunner.Progress;
import io.github.autumntrace.backfill.importer.Imports;
import io.github.autumntrace.backfill.importer.WriterLock;
import io.github.autumntrace.backfill.inventory.Canonical;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The commands. Exit codes: 0 done, 1 compare found a difference, 2 bad usage, 3 refused (the
 * source changed, another writer is active, ...), 137 a controlled crash.
 */
public final class Cli {

    public static final String USAGE = """
            backfill [--schema NAME] <command>

              import <file> [--chunk-size N] [--new] [--quiet]   import a scan log into the schema
              resume <import-id> [--file PATH] [--quiet]          continue an interrupted import from its checkpoint
              status [import-id]                                  imports, progress by byte offset, counts
              compare <schema-a> <schema-b> [--out DIR]           canonical output of two schemas, line by line
              export [--out FILE]                                 canonical output of this schema
              reset                                               drop and recreate this schema
              generate --out FILE [--lines N] [--seed N]          write a seeded synthetic scan log
              bench --lines N [--seed N] [--chunk-sizes 100,1000] import into fresh schemas, one per chunk size

            Controlled failure, for import and resume:
              --crash-at-line N --crash-point before-commit|after-commit
                  halt the JVM (as kill -9 would) in the chunk that contains line N, just before or just after
                  its COMMIT

            Connection: BACKFILL_DB_URL (default jdbc:postgresql://localhost:5432/backfill), BACKFILL_DB_USER,
            BACKFILL_DB_PASSWORD. Schema: --schema, default "backfill".
            """;

    private final PrintStream out;
    private final Database.Settings settings;

    public Cli(PrintStream out, Database.Settings settings) {
        this.out = out;
        this.settings = settings;
    }

    public int run(String[] argv) throws Exception {
        Args args = Args.parse(argv);
        if (args.command == null || args.flags.containsKey("help")) {
            out.print(USAGE);
            return args.command == null ? 2 : 0;
        }
        if (args.command.equals("generate")) {
            return generate(args);
        }
        if (args.command.equals("bench")) {
            return bench(args);
        }
        if (args.command.equals("compare")) {
            return compare(args);
        }
        try (Database db = Database.open(settings, args.flag("schema", "backfill"))) {
            return switch (args.command) {
                case "import" -> importFile(db, args);
                case "resume" -> resume(db, args);
                case "status" -> status(db, args);
                case "export" -> export(db, args);
                case "reset" -> {
                    db.reset();
                    out.println("schema " + db.schema() + " is empty");
                    yield 0;
                }
                default -> {
                    out.print(USAGE);
                    yield 2;
                }
            };
        } catch (ImportRunner.Refused | WriterLock.Busy | CheckpointMoved e) {
            out.println("refused: " + e.getMessage());
            return 3;
        }
    }

    private int importFile(Database db, Args args) throws Exception {
        Path file = Path.of(args.positional(0, "a file to import"));
        int chunkSize = Integer.parseInt(args.flag("chunk-size", "500"));
        ImportRunner runner = new ImportRunner(db);
        long id = runner.start(file, chunkSize, args.flags.containsKey("new"), crash(args), progress(args));
        printImport(db, new Imports(db.jdbc()).find(id).orElseThrow());
        return 0;
    }

    private int resume(Database db, Args args) throws Exception {
        long id = Long.parseLong(args.positional(0, "an import id"));
        Path file = args.flags.containsKey("file") ? Path.of(args.flags.get("file")) : null;
        Imports.Row before = new Imports(db.jdbc()).find(id).orElseThrow(() -> new ImportRunner.Refused("no import " + id));
        out.printf(Locale.ROOT, "resuming import %d at byte %,d (line %,d)%n", id, before.checkpointOffset(),
                before.checkpointLine());
        new ImportRunner(db).resume(id, file, crash(args), progress(args));
        printImport(db, new Imports(db.jdbc()).find(id).orElseThrow());
        return 0;
    }

    private int status(Database db, Args args) throws Exception {
        List<Imports.Row> rows = new Imports(db.jdbc()).all();
        if (!args.positionals.isEmpty()) {
            long id = Long.parseLong(args.positionals.get(0));
            rows = rows.stream().filter(r -> r.id() == id).toList();
        }
        if (rows.isEmpty()) {
            out.println("no imports in schema " + db.schema());
        }
        for (Imports.Row row : rows) {
            printImport(db, row);
        }
        return 0;
    }

    private int export(Database db, Args args) throws IOException {
        List<String> lines = Canonical.of(db.jdbc());
        if (args.flags.containsKey("out")) {
            Files.write(Path.of(args.flags.get("out")), lines, StandardCharsets.UTF_8);
        } else {
            lines.forEach(out::println);
        }
        return 0;
    }

    private int compare(Args args) throws IOException {
        String a = args.positional(0, "two schemas");
        String b = args.positional(1, "two schemas");
        List<String> left;
        List<String> right;
        try (Database da = Database.open(settings, a); Database dbb = Database.open(settings, b)) {
            left = Canonical.of(da.jdbc());
            right = Canonical.of(dbb.jdbc());
        }
        Path dir = Path.of(args.flag("out", "out"));
        Files.createDirectories(dir);
        Files.write(dir.resolve(a + ".txt"), left, StandardCharsets.UTF_8);
        Files.write(dir.resolve(b + ".txt"), right, StandardCharsets.UTF_8);
        out.printf(Locale.ROOT, "%-12s %s%n", a, summary(left));
        out.printf(Locale.ROOT, "%-12s %s%n", b, summary(right));
        String difference = Canonical.firstDifference(left, right);
        if (difference == null) {
            out.println("identical: " + left.size() + " canonical lines (" + dir.resolve(a + ".txt") + ", "
                    + dir.resolve(b + ".txt") + ")");
            return 0;
        }
        out.println("DIFFERENT at " + difference);
        return 1;
    }

    private int generate(Args args) throws IOException {
        Path file = Path.of(args.flag("out", "data/scans.ndjson"));
        var summary = Generator.write(file, Integer.parseInt(args.flag("lines", "12000")),
                Long.parseLong(args.flag("seed", "20261126")));
        out.printf(Locale.ROOT, "%s: %,d lines, %,d new scans, %,d repeated scans, %,d malformed, event %s reused at line %,d%n",
                file, summary.lines(), summary.scans(), summary.repeats(), summary.malformed(), summary.conflictEventId(),
                summary.conflictLine());
        return 0;
    }

    private int bench(Args args) throws Exception {
        int lines = Integer.parseInt(args.flag("lines", "200000"));
        long seed = Long.parseLong(args.flag("seed", "7"));
        Path file = Path.of(args.flag("file", "data/bench-" + lines + "-" + seed + ".ndjson"));
        if (!Files.exists(file)) {
            Generator.write(file, lines, seed);
        }
        long bytes = Files.size(file);
        out.printf(Locale.ROOT, "%s: %,d lines, %,d bytes, seed %d%n", file, lines, bytes, seed);
        // One unmeasured pass first, so the first measured chunk size does not pay for JIT warm-up.
        try (Database warm = Database.open(settings, "bench_warmup")) {
            warm.reset();
            new ImportRunner(warm).start(file, 1000, true, Crash.NONE, p -> {});
        }
        out.println("| chunk size | chunks | wall time | lines/s | median chunk | slowest chunk | most work redone after a crash |");
        out.println("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
        for (String size : args.flag("chunk-sizes", "100,1000,10000").split(",")) {
            int chunkSize = Integer.parseInt(size.trim());
            try (Database db = Database.open(settings, "bench_" + chunkSize)) {
                db.reset();
                List<Long> chunkNanos = new ArrayList<>();
                long[] last = {System.nanoTime()};
                long started = last[0];
                long id = new ImportRunner(db).start(file, chunkSize, true, Crash.NONE, p -> {
                    long now = System.nanoTime();
                    chunkNanos.add(now - last[0]);
                    last[0] = now;
                });
                double seconds = (System.nanoTime() - started) / 1e9;
                // The first interval also holds the SHA-256 of the whole file; leave it out.
                List<Long> sorted = chunkNanos.subList(1, chunkNanos.size()).stream().sorted().toList();
                Imports.Row row = new Imports(db.jdbc()).find(id).orElseThrow();
                out.printf(Locale.ROOT, "| %,d | %,d | %.2f s | %,.0f | %.1f ms | %.1f ms | %,d lines |%n", chunkSize,
                        row.chunksCommitted(), seconds, lines / seconds,
                        sorted.isEmpty() ? 0 : sorted.get(sorted.size() / 2) / 1e6,
                        sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1) / 1e6, chunkSize);
            }
        }
        return 0;
    }

    private Crash crash(Args args) {
        if (!args.flags.containsKey("crash-at-line")) {
            return Crash.NONE;
        }
        long line = Long.parseLong(args.flags.get("crash-at-line"));
        Point point = switch (args.flag("crash-point", "before-commit")) {
            case "before-commit" -> Point.BEFORE_COMMIT;
            case "after-commit" -> Point.AFTER_COMMIT;
            default -> throw new IllegalArgumentException("--crash-point is before-commit or after-commit");
        };
        return (at, chunk) -> {
            if (at == point && chunk.startLine() < line && line <= chunk.endLine()) {
                out.printf(Locale.ROOT, "CRASH %s of the chunk holding lines %,d-%,d (halting the JVM)%n",
                        point == Point.BEFORE_COMMIT ? "before the COMMIT" : "after the COMMIT", chunk.startLine() + 1,
                        chunk.endLine());
                out.flush();
                Runtime.getRuntime().halt(137);
            }
        };
    }

    private Consumer<Progress> progress(Args args) {
        if (args.flags.containsKey("quiet")) {
            return p -> {};
        }
        return p -> out.printf(Locale.ROOT, "chunk lines %,7d-%,7d  byte %,11d / %,d (%5.1f%%)  +%d accepted, %d duplicate, %d conflict, %d rejected%n",
                p.chunk().startLine() + 1, p.chunk().endLine(), p.chunk().endOffset(), p.sourceBytes(),
                p.sourceBytes() == 0 ? 100.0 : 100.0 * p.chunk().endOffset() / p.sourceBytes(),
                p.outcome().accepted(), p.outcome().duplicates(), p.outcome().conflicts(), p.outcome().rejected());
    }

    private void printImport(Database db, Imports.Row row) throws Exception {
        String state = row.state();
        if (!row.completed()) {
            state = WriterLock.held(db.dataSource(), row.id()) ? "running (a writer holds it)" : "interrupted (no writer; resume it)";
        }
        DateTimeFormatter time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        out.printf(Locale.ROOT, "import %d  %s%n", row.id(), state);
        out.printf(Locale.ROOT, "  source     %s  (sha256 %s..., %,d bytes)%n", row.sourcePath(), row.sourceSha256().substring(0, 12),
                row.sourceBytes());
        out.printf(Locale.ROOT, "  checkpoint byte %,d of %,d (%.1f%%), %,d lines, %,d chunks of %,d committed%n",
                row.checkpointOffset(), row.sourceBytes(),
                row.sourceBytes() == 0 ? 100.0 : 100.0 * row.checkpointOffset() / row.sourceBytes(), row.checkpointLine(),
                row.chunksCommitted(), row.chunkSize());
        out.printf(Locale.ROOT, "  events     %,d accepted, %,d duplicates, %,d conflicts (quarantined), %,d rejected%n",
                row.accepted(), row.duplicates(), row.conflicts(), row.rejected());
        out.printf(Locale.ROOT, "  updated    %s by %s%n", row.updatedAt().format(time), row.lastWorker() == null ? "-" : row.lastWorker());
    }

    private static String summary(List<String> canonical) {
        Map<String, String> head = new HashMap<>();
        for (String line : canonical) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                head.put(parts[0], parts[1]);
            }
        }
        return String.format(Locale.ROOT, "applied %s, duplicates %s, conflicts %s, rejected %s, %s stock rows totalling %s",
                head.get("events applied"), head.get("duplicates"), head.get("conflicts"), head.get("rejected"),
                head.get("stock rows"), head.get("stock total"));
    }

    /** --flag value, --flag=value and bare --flag, plus positionals. */
    static final class Args {
        String command;
        final List<String> positionals = new ArrayList<>();
        final Map<String, String> flags = new HashMap<>();

        static Args parse(String[] argv) {
            Args args = new Args();
            for (int i = 0; i < argv.length; i++) {
                String a = argv[i];
                if (a.startsWith("--")) {
                    String name = a.substring(2);
                    if (name.contains("=")) {
                        args.flags.put(name.substring(0, name.indexOf('=')), name.substring(name.indexOf('=') + 1));
                    } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--") && !List.of("new", "quiet", "help").contains(name)) {
                        args.flags.put(name, argv[++i]);
                    } else {
                        args.flags.put(name, "");
                    }
                } else if (args.command == null) {
                    args.command = a;
                } else {
                    args.positionals.add(a);
                }
            }
            return args;
        }

        String flag(String name, String fallback) {
            String value = flags.get(name);
            return value == null || value.isEmpty() ? fallback : value;
        }

        String positional(int index, String what) {
            if (positionals.size() <= index) {
                throw new IllegalArgumentException("expected " + what);
            }
            return positionals.get(index);
        }
    }
}

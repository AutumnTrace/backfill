package io.github.autumntrace.backfill;

import static io.github.autumntrace.backfill.Fixtures.DEMO;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The real thing: the CLI in its own JVM, halted (Runtime.halt, no shutdown hooks, connections
 * dropped mid-transaction) around a COMMIT, then the CLI resumed in a second JVM, then compare.
 */
class ProcessCrashTest {

    record Run(int exit, String output) {}

    static Run cli(String... args) throws IOException, InterruptedException {
        Database.Settings settings = TestDatabase.settings();
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> command = new ArrayList<>(List.of(java, "-cp", System.getProperty("java.class.path"),
                BackfillApplication.class.getName()));
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("BACKFILL_DB_URL", settings.url());
        builder.environment().put("BACKFILL_DB_USER", settings.user());
        builder.environment().put("BACKFILL_DB_PASSWORD", settings.password());
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(120, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("cli did not finish: " + output);
        }
        return new Run(process.exitValue(), output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"before-commit", "after-commit"})
    void a_halted_worker_resumes_to_the_reference_result(String point) throws Exception {
        String suffix = Long.toString(System.nanoTime(), 36);
        String reference = "proc_ref_" + suffix;
        String crashed = "proc_crash_" + suffix;

        assertThat(cli("--schema", reference, "import", DEMO.toString(), "--quiet").exit()).isZero();

        Run crash = cli("--schema", crashed, "import", DEMO.toString(), "--quiet", "--crash-at-line", "6000", "--crash-point", point);
        assertThat(crash.exit()).as(crash.output()).isEqualTo(137);
        assertThat(crash.output()).contains("CRASH");

        // The dead worker's connection is gone, and so is its advisory lock.
        Run status = cli("--schema", crashed, "status");
        assertThat(status.output()).contains("interrupted (no writer; resume it)")
                .contains(point.equals("before-commit") ? "5,500 lines" : "6,000 lines");

        Run resume = cli("--schema", crashed, "resume", "1", "--quiet");
        assertThat(resume.exit()).as(resume.output()).isZero();

        Run compare = cli("compare", reference, crashed, "--out", "target/compare-" + suffix);
        assertThat(compare.exit()).as(compare.output()).isZero();
        assertThat(compare.output()).contains("identical");
    }
}

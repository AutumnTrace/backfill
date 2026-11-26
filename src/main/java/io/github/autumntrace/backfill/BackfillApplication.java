package io.github.autumntrace.backfill;

import io.github.autumntrace.backfill.cli.Cli;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BackfillApplication {

    private static int exitCode;

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(BackfillApplication.class);
        System.exit(SpringApplication.exit(app.run(args)));
    }

    @Bean
    ApplicationRunner cli() {
        return args -> {
            try {
                exitCode = new Cli(System.out, Database.Settings.fromEnvironment()).run(args.getSourceArgs());
            } catch (IllegalArgumentException e) {
                System.out.println("usage: " + e.getMessage());
                System.out.print(Cli.USAGE);
                exitCode = 2;
            }
        };
    }

    @Bean
    ExitCodeGenerator exitCodes() {
        return () -> exitCode;
    }
}

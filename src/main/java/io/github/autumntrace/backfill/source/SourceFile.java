package io.github.autumntrace.backfill.source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The identity of a source file: its path, size and SHA-256. An import records it at the start;
 * a resume recomputes it and refuses to continue if anything changed, because a byte offset into
 * a different file means nothing.
 */
public record SourceFile(Path path, String sha256, long bytes) {

    public static SourceFile inspect(Path path) throws IOException {
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        long size = 0;
        try (InputStream in = new DigestInputStream(Files.newInputStream(path), sha256)) {
            byte[] buffer = new byte[1 << 16];
            for (int n; (n = in.read(buffer)) != -1; ) {
                size += n;
            }
        }
        return new SourceFile(path.toAbsolutePath().normalize(), HexFormat.of().formatHex(sha256.digest()), size);
    }

    public String shortSha() {
        return sha256.substring(0, 12);
    }
}

package io.github.autumntrace.backfill.source;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads newline-delimited records from a byte offset, keeping exact byte positions.
 *
 * <p>A record ends at '\n'; a trailing '\r' is dropped. The last record may end at end of file
 * without a newline. Offsets are bytes, not characters, so they stay valid for UTF-8 content.
 * Lines longer than {@link #MAX_LINE_BYTES} are still consumed to their end, so the next offset is
 * right, but only their first bytes are kept.
 */
public final class LineReader implements Closeable {

    public static final int MAX_LINE_BYTES = 8192;

    /** One record: its 1-based line number, where it starts, where the next one starts. */
    public record Line(long lineNo, long offset, long nextOffset, String text, boolean truncated) {}

    /** Up to chunkSize lines; atEnd when the file has nothing after the last of them. */
    public record Chunk(long startOffset, long startLine, List<Line> lines, long endOffset, long endLine, boolean atEnd) {}

    private final InputStream in;
    private long offset;
    private long lineNo;
    private int peeked = -2;

    private LineReader(InputStream in, long offset, long lineNo) {
        this.in = in;
        this.offset = offset;
        this.lineNo = lineNo;
    }

    /** Opens the file positioned at a checkpoint: the byte offset and the number of lines before it. */
    public static LineReader open(Path path, long offset, long linesBefore) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        channel.position(offset);
        return new LineReader(new BufferedInputStream(Channels.newInputStream(channel), 1 << 16), offset, linesBefore);
    }

    public Chunk readChunk(int chunkSize) throws IOException {
        long startOffset = offset;
        long startLine = lineNo;
        List<Line> lines = new ArrayList<>(Math.min(chunkSize, 4096));
        while (lines.size() < chunkSize) {
            Line line = next();
            if (line == null) {
                break;
            }
            lines.add(line);
        }
        return new Chunk(startOffset, startLine, lines, offset, lineNo, peek() == -1);
    }

    private Line next() throws IOException {
        if (peek() == -1) {
            return null;
        }
        long start = offset;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
        boolean truncated = false;
        int b;
        while ((b = read()) != -1 && b != '\n') {
            if (bytes.size() < MAX_LINE_BYTES) {
                bytes.write(b);
            } else {
                truncated = true;
            }
        }
        byte[] raw = bytes.toByteArray();
        int length = raw.length > 0 && raw[raw.length - 1] == '\r' && !truncated ? raw.length - 1 : raw.length;
        lineNo++;
        return new Line(lineNo, start, offset, new String(raw, 0, length, StandardCharsets.UTF_8), truncated);
    }

    private int peek() throws IOException {
        if (peeked == -2) {
            peeked = in.read();
        }
        return peeked;
    }

    private int read() throws IOException {
        int b = peek();
        peeked = -2;
        if (b != -1) {
            offset++;
        }
        return b;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}

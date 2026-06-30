package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards against the source corruption behind code-review finding #8: {@code GetObjectsBuilder.kt}
 * had embedded NUL bytes (its map-key separator was a NUL, not the space it appeared to be), which
 * also made git treat the file as binary. This offline test fails if any Kotlin source in the
 * module contains a NUL byte.
 */
class SourceHygieneTest {

    @Test
    void noNulBytesInSources() throws IOException {
        Path sourceRoot = Path.of("src");
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            for (Path file : (Iterable<Path>) files
                    .filter(p -> {
                        String n = p.toString();
                        return n.endsWith(".kt") || n.endsWith(".java");
                    })::iterator) {
                byte[] bytes = Files.readAllBytes(file);
                for (byte b : bytes) {
                    if (b == 0) {
                        offenders.add(sourceRoot.relativize(file).toString());
                        break;
                    }
                }
            }
        }
        assertEquals(List.of(), offenders, "Kotlin/Java sources must not contain NUL bytes");
    }
}

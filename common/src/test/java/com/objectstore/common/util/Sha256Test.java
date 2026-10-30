package com.objectstore.common.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Sha256Test {

    private static final String EMPTY_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    @Test
    void emptyByteArrayAndStreamProduceKnownVector() throws Exception {
        assertThat(Sha256.hash(new byte[0])).isEqualTo(EMPTY_SHA256);
        assertThat(Sha256.hash(new ByteArrayInputStream(new byte[0]))).isEqualTo(EMPTY_SHA256);
    }

    @Test
    void copyAndHashCopiesExactBytesAndReturnsDigest() throws Exception {
        byte[] input = "Mini Distributed Cloud Object Store".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Sha256.StreamHashResult res = Sha256.copyAndHash(new ByteArrayInputStream(input), out);

        assertThat(res.bytesWritten()).isEqualTo(input.length);
        assertThat(out.toByteArray()).isEqualTo(input);
        assertThat(res.sha256Hex()).isEqualTo(Sha256.hash(input));
    }

    @Test
    void normalizeHexAcceptsPrefixAndUpperCase() {
        String upper = EMPTY_SHA256.toUpperCase();
        assertThat(Sha256.normalizeHex(upper)).isEqualTo(EMPTY_SHA256);
        assertThat(Sha256.normalizeHex("sha256:" + upper)).isEqualTo(EMPTY_SHA256);
        assertThat(Sha256.normalizeHex("  SHA256:" + EMPTY_SHA256 + " ")).isEqualTo(EMPTY_SHA256);
    }

    @Test
    void normalizeHexRejectsInvalidStrings() {
        assertThatThrownBy(() -> Sha256.normalizeHex(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Sha256.normalizeHex("short")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Sha256.normalizeHex("z".repeat(64))).isInstanceOf(IllegalArgumentException.class);
        assertThat(Sha256.isValidHex("not-a-hash")).isFalse();
        assertThat(Sha256.isValidHex(EMPTY_SHA256)).isTrue();
    }
}


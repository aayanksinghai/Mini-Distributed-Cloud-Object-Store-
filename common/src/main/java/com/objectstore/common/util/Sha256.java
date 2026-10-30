package com.objectstore.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * SHA-256 hashing and hex formatting helpers used by both controller and workers.
 */
public final class Sha256 {

    private static final Pattern HEX_64 = Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final HexFormat HEX = HexFormat.of();

    private Sha256() {
    }

    public static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available in JVM", e);
        }
    }

    public static String hash(byte[] data) {
        MessageDigest md = newDigest();
        return HEX.formatHex(md.digest(data));
    }

    /**
     * Copies all bytes from {@code in} to {@code out} while computing the SHA-256 digest.
     *
     * @return the number of bytes copied and the lowercase 64-char hex SHA-256 digest
     */
    public static StreamHashResult copyAndHash(InputStream in, OutputStream out) throws IOException {
        MessageDigest md = newDigest();
        byte[] buffer = new byte[64 * 1024];
        long total = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            md.update(buffer, 0, read);
            out.write(buffer, 0, read);
            total += read;
        }
        return new StreamHashResult(total, HEX.formatHex(md.digest()));
    }

    public static String hash(InputStream in) throws IOException {
        MessageDigest md = newDigest();
        byte[] buffer = new byte[64 * 1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            md.update(buffer, 0, read);
        }
        return HEX.formatHex(md.digest());
    }

    /**
     * Normalises a SHA-256 header/value (optionally prefixed with {@code sha256:}) to lowercase 64-char hex.
     *
     * @throws IllegalArgumentException if the input is not a valid 64-character hex string
     */
    public static String normalizeHex(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Checksum must not be null");
        }
        String trimmed = raw.trim();
        if (trimmed.regionMatches(true, 0, "sha256:", 0, 7)) {
            trimmed = trimmed.substring(7).trim();
        }
        if (!HEX_64.matcher(trimmed).matches()) {
            throw new IllegalArgumentException("Invalid SHA-256 hex digest: " + raw);
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    public static boolean isValidHex(String raw) {
        if (raw == null) {
            return false;
        }
        String trimmed = raw.trim();
        if (trimmed.regionMatches(true, 0, "sha256:", 0, 7)) {
            trimmed = trimmed.substring(7).trim();
        }
        return HEX_64.matcher(trimmed).matches();
    }

    public record StreamHashResult(long bytesWritten, String sha256Hex) {
    }
}


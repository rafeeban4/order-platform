package dev.rafee.orders.ingest;

import java.security.SecureRandom;

/**
 * Minimal ULID generator (Crockford base32, 48-bit timestamp + 80-bit
 * randomness). Sortable by creation time, generated without any shared
 * state or DB round-trip — which is why the ingest hot path can assign
 * order ids itself.
 */
public final class Ulid {

    private static final char[] ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();

    private Ulid() {
    }

    public static String next() {
        long time = System.currentTimeMillis();
        char[] out = new char[26];
        for (int i = 9; i >= 0; i--) {
            out[i] = ALPHABET[(int) (time & 31)];
            time >>>= 5;
        }
        byte[] rand = new byte[16];
        RANDOM.nextBytes(rand);
        // 16 base32 chars of randomness from 80 bits (use 5 bits per byte for simplicity)
        for (int i = 0; i < 16; i++) {
            out[10 + i] = ALPHABET[rand[i] & 31];
        }
        return new String(out);
    }
}

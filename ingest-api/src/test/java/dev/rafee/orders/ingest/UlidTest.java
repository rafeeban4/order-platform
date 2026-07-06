package dev.rafee.orders.ingest;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UlidTest {

    @Test
    void isTwentySixCrockfordChars() {
        String id = Ulid.next();
        assertEquals(26, id.length());
        assertTrue(id.matches("[0-9A-HJKMNP-TV-Z]{26}"), "not Crockford base32: " + id);
    }

    @Test
    void sortsByCreationTime() throws InterruptedException {
        String first = Ulid.next();
        Thread.sleep(2); // ULID time component has millisecond resolution
        String second = Ulid.next();
        assertTrue(first.compareTo(second) < 0, first + " !< " + second);
    }

    @Test
    void noCollisionsInTightLoop() {
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            assertTrue(seen.add(Ulid.next()), "collision at iteration " + i);
        }
    }
}

package net.coreprotect.utility.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BackgroundServiceTest {

    @Test
    void parsesCommandStyleRetentionValues() {
        assertEquals(86400L, BackgroundService.parseRetention("1d"));
        assertEquals(15552000L, BackgroundService.parseRetention("180d"));
        assertEquals(7257600L, BackgroundService.parseRetention("12w"));
        assertEquals(15552000L, BackgroundService.parseRetention("6mo"));
        assertEquals(31536000L, BackgroundService.parseRetention("1y"));
        assertEquals(90000L, BackgroundService.parseRetention("1d1h"));
    }

    @Test
    void readsPlainNumbersAsDays() {
        assertEquals(15552000L, BackgroundService.parseRetention("180"));
    }

    @Test
    void treatsDisabledAndInvalidValuesAsUnset() {
        assertEquals(0L, BackgroundService.parseRetention("false"));
        assertEquals(0L, BackgroundService.parseRetention(""));
        assertEquals(0L, BackgroundService.parseRetention(null));
        assertEquals(0L, BackgroundService.parseRetention("later"));
        assertEquals(0L, BackgroundService.parseRetention("30q"));
    }
}

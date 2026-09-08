package net.coreprotect.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AutoPurgeServiceTest {

    @Test
    void parsesCommandStyleRetentionValues() {
        assertEquals(86400L, AutoPurgeService.parseRetention("1d"));
        assertEquals(15552000L, AutoPurgeService.parseRetention("180d"));
        assertEquals(7257600L, AutoPurgeService.parseRetention("12w"));
        assertEquals(15552000L, AutoPurgeService.parseRetention("6mo"));
        assertEquals(31536000L, AutoPurgeService.parseRetention("1y"));
        assertEquals(90000L, AutoPurgeService.parseRetention("1d1h"));
    }

    @Test
    void readsPlainNumbersAsDays() {
        assertEquals(15552000L, AutoPurgeService.parseRetention("180"));
    }

    @Test
    void treatsDisabledAndInvalidValuesAsUnset() {
        assertEquals(0L, AutoPurgeService.parseRetention("false"));
        assertEquals(0L, AutoPurgeService.parseRetention(""));
        assertEquals(0L, AutoPurgeService.parseRetention(null));
        assertEquals(0L, AutoPurgeService.parseRetention("later"));
        assertEquals(0L, AutoPurgeService.parseRetention("30q"));
    }
}

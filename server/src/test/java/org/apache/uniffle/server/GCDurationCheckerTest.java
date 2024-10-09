package org.apache.uniffle.server;

import org.apache.uniffle.common.util.GCEvent;
import org.apache.uniffle.common.util.SlidingTimeWindow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GCDurationCheckerTest {

    @Test
    public void test() throws InterruptedException {
        SlidingTimeWindow timeWindow = new SlidingTimeWindow(1 * 1000L);
        GCDurationChecker checker = new GCDurationChecker(
            timeWindow,
            1 * 1000L
        );

        // case1
        timeWindow.addEvent(new GCEvent(1, 500));
        assertTrue(checker.checkIsHealthy());
        timeWindow.addEvent(new GCEvent(1, 1000));
        assertFalse(checker.checkIsHealthy());

        // case2
        checker.reset();
        Thread.sleep(2 * 1000);
        assertEquals(0, timeWindow.getCount());
        assertEquals(0, timeWindow.getTotalGCDurationMillis());
        assertTrue(checker.checkIsHealthy());
    }
}

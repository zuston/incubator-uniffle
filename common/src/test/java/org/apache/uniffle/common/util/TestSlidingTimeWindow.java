package org.apache.uniffle.common.util;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSlidingTimeWindow {
    @Test
    public void test() throws InterruptedException {
        SlidingTimeWindow window = new SlidingTimeWindow(5000);

        window.addEvent(new GCEvent(1, 1));
        Thread.sleep(1000);
        window.addEvent(new GCEvent(1, 1));
        Thread.sleep(1000);
        window.addEvent(new GCEvent(1, 1));
        Thread.sleep(5000);
        window.addEvent(new GCEvent(1, 1));

        assertEquals(1, window.getCount());
        assertEquals(1, window.getTotalGCDurationMillis());
    }
}

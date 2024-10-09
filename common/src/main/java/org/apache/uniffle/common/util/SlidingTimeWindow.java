package org.apache.uniffle.common.util;

import java.util.Deque;
import java.util.LinkedList;

public class SlidingTimeWindow {
    private final long windowSizeInMillis;
    private final Deque<GCEvent> events;

    public SlidingTimeWindow(long windowSizeInMillis) {
        this.windowSizeInMillis = windowSizeInMillis;
        this.events = new LinkedList<>();
    }

    public synchronized void addEvent(GCEvent event) {
        events.addLast(event);
        cleanup();
    }

    public synchronized int getCount() {
        cleanup();
        return events.size();
    }

    public synchronized long getTotalGCDurationMillis() {
        cleanup();
        return events.stream().map(x -> x.getGcDurationMillis()).reduce(0L, Long::sum);
    }

    private void cleanup() {
        while (!events.isEmpty() && events.peekFirst().getEventTimeMillis() < System.currentTimeMillis() - windowSizeInMillis) {
            events.removeFirst();
        }
    }
}

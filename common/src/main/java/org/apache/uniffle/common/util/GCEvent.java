package org.apache.uniffle.common.util;

public class GCEvent {
    private long eventTimeMillis;
    private int gcCount;
    private long gcDurationMillis;

    public GCEvent(int gcCount, long gcDurationMillis) {
        this.gcCount = gcCount;
        this.gcDurationMillis = gcDurationMillis;
        this.eventTimeMillis = System.currentTimeMillis();
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    public int getGcCount() {
        return gcCount;
    }

    public void setGcCount(int gcCount) {
        this.gcCount = gcCount;
    }

    public long getGcDurationMillis() {
        return gcDurationMillis;
    }

    public void setGcDurationMillis(long gcDurationMillis) {
        this.gcDurationMillis = gcDurationMillis;
    }
}

package org.apache.uniffle.server;

import com.google.common.annotations.VisibleForTesting;
import org.apache.uniffle.common.util.SlidingTimeWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCDurationChecker extends Checker {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCDurationChecker.class);
    private final SlidingTimeWindow slidingTimeWindow;
    private boolean isHealthy = true;

    private final long gcDurationThresholdMillis;

    public GCDurationChecker(ShuffleServerConf conf) {
        super(conf);
        this.slidingTimeWindow = ShuffleServer.jvmPauseMonitor.getSlidingTimeWindow();
        this.gcDurationThresholdMillis = conf.getLong("rss.server.health.checker.gcDurationChecker.healthyDurationMillis", 40 * 1000L);
    }

    // only for test
    @VisibleForTesting
    public GCDurationChecker(SlidingTimeWindow timeWindow, long gcDurationThresholdMillis) {
        super(new ShuffleServerConf());
        this.slidingTimeWindow = timeWindow;
        this.gcDurationThresholdMillis = gcDurationThresholdMillis;
    }

    @Override
    public boolean checkIsHealthy() {
        if (!isHealthy) {
            return false;
        }

        long duration = slidingTimeWindow.getTotalGCDurationMillis();
        LOGGER.debug("gcCount: {}, gcDuration: {}(ms), threshold: {}(ms)", slidingTimeWindow.getCount(), duration, gcDurationThresholdMillis);
        if (duration > gcDurationThresholdMillis) {
            this.isHealthy = false;
            LOGGER.error("Detected GC duration {} > {} in one sliding window. Make it unhealthy!", duration, gcDurationThresholdMillis);
            return false;
        }
        return true;
    }

    public void reset() {
        this.isHealthy = true;
    }
}

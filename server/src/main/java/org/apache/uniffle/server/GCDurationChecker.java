package org.apache.uniffle.server;

import com.google.common.annotations.VisibleForTesting;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.util.SlidingTimeWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCDurationChecker extends Checker {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCDurationChecker.class);
    private final SlidingTimeWindow slidingTimeWindow;
    private boolean isHealthy = true;

    private final ReconfigurableConfManager.Reconfigurable<Long> gcDurationThresholdMillis;

    private static final ConfigOption<Long> OPTION = ConfigOptions.key("rss.server.health.checker.gcDurationChecker.healthyDurationMillis")
        .longType()
        .defaultValue(40 * 1000L);

    public GCDurationChecker(ShuffleServerConf conf) {
        super(conf);
        this.slidingTimeWindow = ShuffleServer.jvmPauseMonitor.getSlidingTimeWindow();

        this.gcDurationThresholdMillis = ReconfigurableConfManager.register(conf, OPTION);
    }

    // only for test
    @VisibleForTesting
    public GCDurationChecker(SlidingTimeWindow timeWindow, long gcDurationThresholdMillis) {
        super(new ShuffleServerConf());
        this.slidingTimeWindow = timeWindow;
        this.gcDurationThresholdMillis = new ReconfigurableConfManager.FixedReconfigurable<>(
            new ShuffleServerConf(),
            ConfigOptions.key("rss.server.health.checker.gcDurationChecker.healthyDurationMillis")
                .longType()
                .defaultValue(gcDurationThresholdMillis)
        );
    }

    @Override
    public boolean checkIsHealthy() {
        if (!isHealthy) {
            return false;
        }

        long duration = slidingTimeWindow.getTotalGCDurationMillis();
        LOGGER.debug("gcCount: {}, gcDuration: {}(ms), threshold: {}(ms)", slidingTimeWindow.getCount(), duration, gcDurationThresholdMillis.get());
        if (duration > gcDurationThresholdMillis.get()) {
            this.isHealthy = false;
            LOGGER.error("Detected GC duration {} > {} in one sliding window. Make it unhealthy!", duration, gcDurationThresholdMillis.get());
            return false;
        }
        return true;
    }

    public void reset() {
        this.isHealthy = true;
    }
}

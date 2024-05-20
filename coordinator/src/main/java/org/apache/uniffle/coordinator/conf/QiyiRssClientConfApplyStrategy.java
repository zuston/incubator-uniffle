package org.apache.uniffle.coordinator.conf;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QiyiRssClientConfApplyStrategy extends AbstractRssClientConfApplyStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(QiyiRssClientConfApplyStrategy.class);

    private final String whiteListPath = "/usr/lib/uniffle/conf/dynamic_conf_apply.whitelist";
    private final Set<String> whiteListUsers = new HashSet<>();

    public QiyiRssClientConfApplyStrategy(DynamicClientConfService dynamicClientConfService) {
        super(dynamicClientConfService);
        loadWhiteList();
    }

    private void loadWhiteList() {
        try {
            List<String> lines = Files.readAllLines(Paths.get(whiteListPath));
            if (CollectionUtils.isNotEmpty(lines)) {
                this.whiteListUsers.addAll(
                    lines.stream().map(x -> x.trim())
                        .filter(x -> StringUtils.isNotBlank(x))
                        .collect(Collectors.toSet()));
                LOGGER.info("Whitelist users: {}", whiteListUsers);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    Map<String, String> apply(RssClientConfFetchInfo rssClientConfFetchInfo) {
        String user = rssClientConfFetchInfo.getUser();
        if (whiteListUsers.contains(user)) {
            LOGGER.info("Hit. user: {}, properties: {}", user, rssClientConfFetchInfo.getProperties());
            return dynamicClientConfService.getRssClientConf();
        }
        return Collections.EMPTY_MAP;
    }
}

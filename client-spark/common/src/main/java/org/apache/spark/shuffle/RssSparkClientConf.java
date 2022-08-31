package org.apache.spark.shuffle;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import scala.Tuple2;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssClientConf;

public class RssSparkClientConf extends RssClientConf {
  public static final String SPARK_CONFIG_KEY_PREFIX = "spark.";
  public static final String SPARK_CONFIG_RSS_KEY_PREFIX = SPARK_CONFIG_KEY_PREFIX + "rss.";

  private RssSparkClientConf(SparkConf sparkConf) {
    List<ConfigOption<Object>> configOptions = ConfigUtils.getAllConfigOptions(RssClientConf.class);

    Map<String, ConfigOption<Object>> configOptionMap = configOptions
        .stream()
        .collect(
            Collectors.toMap(
                entry -> entry.key(),
                entry -> entry
            )
        );

    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      String key = tuple._1;
      if (!key.startsWith(SPARK_CONFIG_RSS_KEY_PREFIX)) {
        continue;
      }
      key = key.substring(SPARK_CONFIG_KEY_PREFIX.length());
      String val = tuple._2;
      ConfigOption configOption = configOptionMap.get(key);
      if (configOption != null) {
        set(configOption, ConfigUtils.convertValue(val, configOption.getClazz()));
      }
    }
  }

  public static RssSparkClientConf from(SparkConf sparkConf) {
    return new RssSparkClientConf(sparkConf);
  }
}

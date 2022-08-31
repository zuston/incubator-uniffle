package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssClientConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RssSparkClientConfTest {

  @Test
  public void testLoadFromSparkConf() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.rss.client.compression.type", "LZ4");
    sparkConf.set("spark.master", "yarn");
    RssClientConf clientConf = RssSparkClientConf.from(sparkConf);
    String type = clientConf.get(COMPRESSION_TYPE);
    assertEquals("LZ4", type);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.config;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssConfTest {

  @Test
  public void testA() {
    Roaring64NavigableMap map1 = new Roaring64NavigableMap();
    map1.addLong(1L);
    map1.addLong(2L);

    Roaring64NavigableMap map2 = new Roaring64NavigableMap();
    map2.addLong(2L);
    map2.addLong(3L);

    map1.and(map2);
    System.out.println(map1.toString());


    Roaring64NavigableMap map3 = new Roaring64NavigableMap();
    map3.addLong(1L);
    map3.addLong(2L);

    Roaring64NavigableMap map4 = new Roaring64NavigableMap();
    map4.addLong(2L);
    map4.addLong(3L);

    map3.or(map4);
    map3.xor(map4);
    System.out.println(map3.toString());

  }

  @Test
  public void testOptionWithDefault() {
    RssConf cfg = new RssConf();
    cfg.setInteger("int-key", 11);
    cfg.setString("string-key", "abc");

    ConfigOption<String> presentStringOption = ConfigOptions
        .key("string-key")
        .stringType()
        .defaultValue("my-beautiful-default");
    ConfigOption<Integer> presentIntOption = ConfigOptions
        .key("int-key")
        .intType()
        .defaultValue(87);

    assertEquals("abc", cfg.getString(presentStringOption));
    assertEquals("abc", cfg.getValue(presentStringOption));

    assertEquals(11, cfg.getInteger(presentIntOption));
    assertEquals("11", cfg.getValue(presentIntOption));
  }

  @Test
  public void testSetStringAndGetConcreteType() {
    RssConf conf = new RssConf();
    conf.setString("boolean-type", "true");
    conf.setString("int-type", "1111");
    conf.setString("long-type", "1000");
    assertTrue(conf.getBoolean("boolean-type", false));
    assertEquals(conf.getInteger("int-type", 100), 1111);
    assertEquals(conf.getLong("long-type", 222L), 1000L);
  }

  @Test
  public void testOptionWithNoDefault() {
    RssConf cfg = new RssConf();
    cfg.setInteger("int-key", 11);
    cfg.setString("string-key", "abc");

    ConfigOption<String> presentStringOption = ConfigOptions
        .key("string-key")
        .stringType()
        .noDefaultValue();

    assertEquals("abc", cfg.getString(presentStringOption));
    assertEquals("abc", cfg.getValue(presentStringOption));

    // test getting default when no value is present

    ConfigOption<String> stringOption = ConfigOptions
        .key("test")
        .stringType()
        .noDefaultValue();

    // getting strings for null should work
    assertNull(cfg.getValue(stringOption));
    assertNull(cfg.getString(stringOption));

    // overriding the null default should work
    assertEquals("override", cfg.getString(stringOption, "override"));
  }

}

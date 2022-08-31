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

package org.apache.uniffle.common.compression;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.apache.uniffle.common.config.RssClientConf.LZ4_COMPRESSION_DIRECT_MEMORY_ENABLED;
import static org.apache.uniffle.common.config.RssClientConf.ZSTD_COMPRESSION_LEVEL;

public class CompressionFactory {

  public enum Type {
    LZ4,
    ZSTD,
  }

  private CompressionFactory() {
    // ignore
  }

  private static class LAZY_HOLDER {
    static final CompressionFactory INSTANCE = new CompressionFactory();
  }

  public static CompressionFactory get() {
    return LAZY_HOLDER.INSTANCE;
  }

  public Compressor getCompressor(RssConf rssConf) {
    String compressionType = rssConf.get(COMPRESSION_TYPE);
    Type type = Type.valueOf(compressionType);

    switch (type) {
      case LZ4:
        return new Lz4Compressor();
      case ZSTD:
        return new ZstdCompressor(rssConf.get(ZSTD_COMPRESSION_LEVEL));
      default:
        throw new IllegalArgumentException("Unknown compression type: " + type);
    }
  }

  public Decompressor getDecompressor(RssConf rssConf) {
    String compressionType = rssConf.get(COMPRESSION_TYPE);
    Type type = Type.valueOf(compressionType);

    switch (type) {
      case LZ4:
        return new Lz4Decompressor(rssConf.get(LZ4_COMPRESSION_DIRECT_MEMORY_ENABLED));
      case ZSTD:
        return new ZstdDecompressor();
      default:
        throw new IllegalArgumentException("Unknown compression type: " + type);
    }
  }
}

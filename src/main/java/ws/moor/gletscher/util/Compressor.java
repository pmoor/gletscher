/*
 * Copyright 2016 Patrick Moor <patrick@moor.ws>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ws.moor.gletscher.util;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compressor {

  private static final byte[] NOT_COMPRESSED = new byte[] {0};
  private static final byte[] GZIP_COMPRESSED = new byte[] {1};

  private static final int COMPRESSION_PROBE_SIZE = 128 << 10; // 128 KB

  public byte[] compress(byte[] data) {
    byte[] blockInMiddle = pickMiddleBlock(data, COMPRESSION_PROBE_SIZE);
    byte[] compressed = gzipCompress(blockInMiddle);
    if (compressed.length < blockInMiddle.length) {
      if (blockInMiddle.length == data.length) {
        // we compressed everything
        return MoreArrays.concatenate(GZIP_COMPRESSED, compressed);
      } else {
        return MoreArrays.concatenate(GZIP_COMPRESSED, gzipCompress(data));
      }
    } else {
      return MoreArrays.concatenate(NOT_COMPRESSED, data);
    }
  }

  public byte[] decompress(byte[] data) {
    Preconditions.checkArgument(data.length >= 1);
    if (MoreArrays.startsWith(data, NOT_COMPRESSED)) {
      return Arrays.copyOfRange(data, 1, data.length);
    } else if (MoreArrays.startsWith(data, GZIP_COMPRESSED)) {
      return gzipDecompress(Arrays.copyOfRange(data, 1, data.length));
    } else {
      throw new IllegalArgumentException("unknown compression scheme: " + data[0]);
    }
  }

  private byte[] pickMiddleBlock(byte[] data, int size) {
    size = Math.min(size, data.length);
    int begin = (data.length - size) / 2;
    return Arrays.copyOfRange(data, begin, begin + size);
  }

  private byte[] gzipDecompress(byte[] data) {
    try {
      GZIPInputStream is = new GZIPInputStream(new ByteArrayInputStream(data));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ByteStreams.copy(is, baos);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] gzipCompress(byte[] block) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GZIPOutputStream os = new GZIPOutputStream(baos);
      os.write(block);
      os.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

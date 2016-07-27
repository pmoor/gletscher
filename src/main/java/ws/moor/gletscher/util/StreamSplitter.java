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

import com.google.common.collect.AbstractIterator;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

public abstract class StreamSplitter {

  public static StreamSplitter rollingHashSplitter(int maxBlockSize) {
    return new RollingHashStreamSplitter(maxBlockSize);
  }

  public static StreamSplitter fixedSizeSplitter(int maxBlockSize) {
    return new FixedSizeStreamSplitter(maxBlockSize);
  }

  protected final int maxBlockSize;

  private StreamSplitter(int maxBlockSize) {
    this.maxBlockSize = maxBlockSize;
  }

  public abstract Iterator<byte[]> split(InputStream is);

  public int getMaxBlockSize() {
    return maxBlockSize;
  }

  private static final class RollingHashStreamSplitter extends StreamSplitter {

    RollingHashStreamSplitter(int maxBlockSize) {
      super(maxBlockSize);
    }

    @Override public Iterator<byte[]> split(InputStream is) {
      return new SplitIterator(new BufferedInputStream(is, 1 << 20));
    }

    private final class SplitIterator extends AbstractIterator<byte[]> {

      private final InputStream is;
      private final RollingHash rollingHash;
      private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(32 << 10);

      private SplitIterator(InputStream is) {
        this.is = is;
        this.rollingHash = new RollingHash();
      }

      @Override protected byte[] computeNext() {
        try {
          int value = is.read();
          if (value < 0) {
            is.close();
            return endOfData();
          }

          while (value >= 0) {
            buffer.write(value);
            if (rollingHash.update(value) || buffer.size() == maxBlockSize) {
              rollingHash.reset();
              break;
            }
            value = is.read();
          }

          byte[] result = buffer.toByteArray();
          buffer.reset();
          return result;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static class FixedSizeStreamSplitter extends StreamSplitter {

    FixedSizeStreamSplitter(int maxBlockSize) {
      super(maxBlockSize);
    }

    @Override public Iterator<byte[]> split(InputStream is) {
      return new SplitIterator(new BufferedInputStream(is, 1 << 20));
    }

    private class SplitIterator extends AbstractIterator<byte[]> {

      private final InputStream is;

      SplitIterator(InputStream is) {
        this.is = is;
      }

      @Override protected byte[] computeNext() {
        try {
          byte[] buffer = new byte[maxBlockSize];
          int offset = 0;
          while (offset < maxBlockSize) {
            int read = is.read(buffer, offset, maxBlockSize - offset);
            if (read < 0) {
              break;
            } else {
              offset += read;
            }
          }

          if (offset > 0) {
            return Arrays.copyOfRange(buffer, 0, offset);
          } else {
            is.close();
            return endOfData();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

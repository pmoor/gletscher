/*
 * Copyright 2018 Patrick Moor <patrick@moor.ws>
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

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.io.BufferedInputStream;
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
    Preconditions.checkArgument(maxBlockSize > 0, "maxBlockSize must be positive");
    this.maxBlockSize = maxBlockSize;
  }

  public abstract Iterator<byte[]> split(InputStream is);

  public int getMaxBlockSize() {
    return maxBlockSize;
  }

  private static final class RollingHashStreamSplitter extends StreamSplitter {

    private static final int READ_CHUNK_SIZE = 64 << 10;

    RollingHashStreamSplitter(int maxBlockSize) {
      super(maxBlockSize);
    }

    @Override
    public Iterator<byte[]> split(InputStream is) {
      return new SplitIterator(is);
    }

    private final class SplitIterator extends AbstractIterator<byte[]> {

      private final InputStream is;
      private final int readChunkSize = Math.min(maxBlockSize, READ_CHUNK_SIZE);
      private final int[] hashState = new int[] {RollingHash.A_INIT, RollingHash.B_INIT};

      private byte[] buffer = new byte[Math.min(maxBlockSize, READ_CHUNK_SIZE)];
      private int validLen = 0;
      private int scanned = 0;
      private boolean eof = false;

      private SplitIterator(InputStream is) {
        this.is = is;
      }

      @Override
      protected byte[] computeNext() {
        try {
          while (true) {
            if (scanned < validLen) {
              int scanEnd = Math.min(validLen, maxBlockSize);
              int splitLen = RollingHash.findSplit(buffer, scanned, scanEnd, hashState);
              if (splitLen > 0) {
                return emitBlock(splitLen);
              }
              scanned = scanEnd;
              if (scanned == maxBlockSize) {
                return emitBlock(maxBlockSize);
              }
            }

            if (eof) {
              if (validLen > 0) {
                byte[] result = Arrays.copyOf(buffer, validLen);
                validLen = 0;
                scanned = 0;
                return result;
              }
              buffer = null;
              is.close();
              return endOfData();
            }

            int space = buffer.length - validLen;
            if (space == 0) {
              int newCap = (int) Math.min((long) maxBlockSize, (long) buffer.length * 2L);
              buffer = Arrays.copyOf(buffer, newCap);
              space = buffer.length - validLen;
            }
            int read = is.read(buffer, validLen, Math.min(space, readChunkSize));
            if (read < 0) {
              eof = true;
            } else {
              validLen += read;
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      private byte[] emitBlock(int length) {
        byte[] result = Arrays.copyOf(buffer, length);
        int leftover = validLen - length;
        if (leftover > 0) {
          System.arraycopy(buffer, length, buffer, 0, leftover);
        }
        validLen = leftover;
        scanned = 0;
        hashState[0] = RollingHash.A_INIT;
        hashState[1] = RollingHash.B_INIT;
        return result;
      }
    }
  }

  private static class FixedSizeStreamSplitter extends StreamSplitter {

    FixedSizeStreamSplitter(int maxBlockSize) {
      super(maxBlockSize);
    }

    @Override
    public Iterator<byte[]> split(InputStream is) {
      return new SplitIterator(new BufferedInputStream(is, 1 << 20));
    }

    private class SplitIterator extends AbstractIterator<byte[]> {

      private final InputStream is;

      SplitIterator(InputStream is) {
        this.is = is;
      }

      @Override
      protected byte[] computeNext() {
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

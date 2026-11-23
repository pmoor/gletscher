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

import java.util.Arrays;

/** Rolling hash implementation with expected run size of 4 MB. */
class RollingHash {

  static final int BUFFER_SIZE = 8192;
  private static final int BUFFER_MODULO_MASK = BUFFER_SIZE - 1;
  static final int A_INIT = 0x987654;
  static final int B_INIT = 0x456789;
  private static final int MASK = 0xffffff;
  private static final int ADLER_MASK = 0x3fffff;

  /**
   * 12-bit bit-reversal lookup table: {@code REV_12[x]} is the 12-bit reversal of {@code x}.
   * Since {@code adler32() == 0} requires {@code (A & 0xfff) == REV_12[(B >>> 12) & 0xfff]}, this
   * rejects 99.976% of positions without computing a full 32-bit {@link Integer#reverse(int)}.
   */
  private static final short[] REV_12 = new short[4096];

  static {
    for (int i = 0; i < REV_12.length; i++) {
      REV_12[i] = (short) (Integer.reverse(i) >>> 20);
    }
  }

  private final byte[] buffer = new byte[BUFFER_SIZE];
  private int next = 0;

  private int A = A_INIT;
  private int B = B_INIT;

  /**
   * Scans {@code buf[from .. to - 1]} continuing from state {@code state[0] = A, state[1] = B},
   * where {@code buf[0]} is the start of the current block (after a rolling hash reset).
   *
   * <p>Because {@code buf} already holds the preceding {@link #BUFFER_SIZE} bytes of the current
   * block, no separate ring buffer is needed; and because {@code MASK} is {@code 2^24 - 1},
   * per-step masking of {@code A} and {@code B} can be omitted.
   *
   * <p>If a split point is found at index {@code pos} ({@code from <= pos < to}), returns the
   * resulting block length {@code pos + 1}. Otherwise updates {@code state[0]} and {@code state[1]}
   * and returns {@code -1}.
   */
  static int findSplit(byte[] buf, int from, int to, int[] state) {
    int A = state[0];
    int B = state[1];
    short[] rev = REV_12;
    int pos = from;

    if (pos < BUFFER_SIZE) {
      int phase1End = Math.min(to, BUFFER_SIZE);
      for (; pos < phase1End; pos++) {
        A += buf[pos] & 0xff;
        B += A - A_INIT;
        if ((A & 0xfff) == rev[(B >>> 12) & 0xfff]) {
          if (((A ^ Integer.reverse(B << 8)) & ADLER_MASK) == 0) {
            return pos + 1;
          }
        }
      }
    }

    for (; pos < to; pos++) {
      int add = buf[pos] & 0xff;
      int drop = buf[pos - BUFFER_SIZE] & 0xff;
      A += add - drop;
      B += A - (drop << 13) - A_INIT;
      if ((A & 0xfff) == rev[(B >>> 12) & 0xfff]) {
        if (((A ^ Integer.reverse(B << 8)) & ADLER_MASK) == 0) {
          return pos + 1;
        }
      }
    }

    state[0] = A;
    state[1] = B;
    return -1;
  }

  public boolean update(int add) {
    Preconditions.checkArgument(0 <= add && add < 256);
    int drop = buffer[next] & 0xff;

    buffer[next] = (byte) add;
    A = (A + add - drop) & MASK;
    B = (B + A - (drop << 13) - A_INIT) & MASK;

    next = (next + 1) & BUFFER_MODULO_MASK;

    return (A & 0xfff) == REV_12[(B >>> 12) & 0xfff] && adler32() == 0;
  }

  int adler32() {
    int B_rev = Integer.reverse(B << 8);
    return (A ^ B_rev) & ADLER_MASK;
  }

  @Override
  public String toString() {
    return String.format("A=%d, B=%d, adler32=%d", A, B, adler32());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !o.getClass().equals(RollingHash.class)) {
      return false;
    }

    RollingHash other = (RollingHash) o;
    if (A != other.A || B != other.B) {
      return false;
    }

    for (int i = 0; i < BUFFER_SIZE; i++) {
      if (buffer[(next + i) & BUFFER_MODULO_MASK]
          != other.buffer[(other.next + i) & BUFFER_MODULO_MASK]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return adler32();
  }

  public void reset() {
    Arrays.fill(buffer, (byte) 0);
    next = 0;
    A = A_INIT;
    B = B_INIT;
  }
}

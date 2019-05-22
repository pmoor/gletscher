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
  private static final int A_INIT = 0x987654;
  private static final int B_INIT = 0x456789;
  private static final int MASK = 0xffffff;
  private static final int ADLER_MASK = 0x3fffff;

  private final int[] buffer = new int[BUFFER_SIZE];
  private int next = 0;

  private int A = A_INIT;
  private int B = B_INIT;

  public boolean update(int add) {
    Preconditions.checkArgument(0 <= add && add < 256);
    int drop = buffer[next];

    buffer[next] = add;
    A = (A + add - drop) & MASK;
    B = (B + A - drop * BUFFER_SIZE - A_INIT) & MASK;

    next = (next + 1) & BUFFER_MODULO_MASK;

    return adler32() == 0;
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
    Arrays.fill(buffer, 0);
    next = 0;
    A = A_INIT;
    B = B_INIT;
  }
}

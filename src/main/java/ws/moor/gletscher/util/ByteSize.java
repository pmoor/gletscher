/*
 * Copyright 2017 Patrick Moor <patrick@moor.ws>
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

public class ByteSize {

  private static final long thresholds[] =
      new long[] {1L << 50, 1L << 40, 1 << 30, 1 << 20, 1 << 10};
  private static final String names[] =
      new String[] {"PB", "TB", "GB", "MB", "KB"};

  private final long bytes;

  private ByteSize(long bytes) {
    Preconditions.checkArgument(bytes >= 0);
    this.bytes = bytes;
  }

  public static ByteSize ofBytes(long bytes) {
    return new ByteSize(bytes);
  }

  @Override
  public String toString() {
    for (int i = 0; i < thresholds.length; i++) {
      if (bytes >= thresholds[i]) {
        return String.format("%1.3f %s", (double) bytes / thresholds[i], names[i]);
      }
    }
    return String.format("%d B", bytes);
  }
}

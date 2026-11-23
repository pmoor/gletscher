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
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize {

  private static final Pattern PARSE_PATTERN =
      Pattern.compile("^([0-9]+)\\s*([a-zA-Z]*)$");
  private static final ImmutableMap<String, Long> SUFFIX_MULTIPLIERS =
      ImmutableMap.<String, Long>builder()
          .put("", 1L)
          .put("B", 1L)
          .put("K", 1L << 10)
          .put("KI", 1L << 10)
          .put("KIB", 1L << 10)
          .put("KB", 1_000L)
          .put("M", 1L << 20)
          .put("MI", 1L << 20)
          .put("MIB", 1L << 20)
          .put("MB", 1_000_000L)
          .put("G", 1L << 30)
          .put("GI", 1L << 30)
          .put("GIB", 1L << 30)
          .put("GB", 1_000_000_000L)
          .put("T", 1L << 40)
          .put("TI", 1L << 40)
          .put("TIB", 1L << 40)
          .put("TB", 1_000_000_000_000L)
          .build();

  private static final long[] thresholds =
      new long[]{1L << 60, 1L << 50, 1L << 40, 1 << 30, 1 << 20, 1 << 10};
  private static final String[] names = new String[]{"EiB", "PiB", "TiB", "GiB", "MiB", "KiB"};

  private final long bytes;

  private ByteSize(long bytes) {
    Preconditions.checkArgument(bytes >= 0);
    this.bytes = bytes;
  }

  public static ByteSize ofBytes(long bytes) {
    return new ByteSize(bytes);
  }

  /** Parses a non-negative byte size such as {@code "1024"}, {@code "10M"}, or {@code "2GiB"}. */
  public static ByteSize parse(String input) {
    Preconditions.checkNotNull(input, "byte size must not be null");
    String trimmed = input.trim();
    Matcher matcher = PARSE_PATTERN.matcher(trimmed);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size: " + input);
    }
    long value;
    try {
      value = Long.parseLong(matcher.group(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Byte size out of range: " + input, e);
    }
    String unit = matcher.group(2).toUpperCase(Locale.ROOT);
    Long multiplier = SUFFIX_MULTIPLIERS.get(unit);
    if (multiplier == null) {
      throw new IllegalArgumentException("Unknown byte size unit: " + matcher.group(2));
    }
    try {
      return new ByteSize(Math.multiplyExact(value, multiplier));
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Byte size out of range: " + input, e);
    }
  }

  public long toBytes() {
    return bytes;
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

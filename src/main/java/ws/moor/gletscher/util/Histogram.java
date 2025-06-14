/*
 * Copyright 2024 Patrick Moor <patrick@moor.ws>
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

import com.google.common.base.Strings;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.function.Function;

public final class Histogram {
  private final long[] lowerBounds;
  private final long[] sums;
  private final long[] mins;
  private final long[] maxs;
  private final int[] counts;

  public Histogram(long... bounds) {
    this.lowerBounds = bounds;
    this.sums = new long[bounds.length + 1];
    this.mins = new long[bounds.length + 1];
    Arrays.fill(mins, Long.MAX_VALUE);
    this.maxs = new long[bounds.length + 1];
    Arrays.fill(maxs, Long.MIN_VALUE);
    this.counts = new int[bounds.length + 1];
  }

  public void add(long value) {
    int index = findBucket(value);
    sums[index] += value;
    mins[index] = Math.min(mins[index], value);
    maxs[index] = Math.max(maxs[index], value);
    counts[index]++;
  }

  public void write(Function<Long, String> formatter, PrintWriter out) {
    int firstBucket = -1;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] > 0) {
        firstBucket = i;
        break;
      }
    }
    if (firstBucket == -1) {
      // No data.
      out.printf("total count: 0  total sum: %s\n", formatter.apply(0l));
      return;
    }

    int lastBucket = -1;
    for (int i = counts.length - 1; i >= firstBucket; i--) {
      if (counts[i] > 0) {
        lastBucket = i;
        break;
      }
    }

    int maxLowerWidth = 0;
    int maxUpperWidth = 0;
    int maxCountWidth = 0;
    int maxSumWidth = 0;
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] == 0) {
        continue;
      }
      maxLowerWidth = Math.max(maxLowerWidth, formatter.apply(i == firstBucket ? mins[i] : lowerBounds[i - 1]).length());
      maxUpperWidth = Math.max(maxUpperWidth, formatter.apply(i == lastBucket ? maxs[i] : lowerBounds[i]).length());
      maxCountWidth = Math.max(maxCountWidth, String.valueOf(counts[i]).length());
      maxSumWidth = Math.max(maxSumWidth, formatter.apply(sums[i]).length());
      minValue = Math.min(mins[i], minValue);
      maxValue = Math.max(maxs[i], maxValue);
    }

    int totalCount = Arrays.stream(counts).sum();
    long totalSum = Arrays.stream(sums).sum();

    out.print(Strings.padStart("counts", 8 + maxLowerWidth + maxUpperWidth + maxCountWidth, ' '));
    out.print(Strings.padStart("sums", 12 + maxSumWidth, ' '));
    out.println();
    for (int i = 0; i <= lastBucket; i++) {
      if (counts[i] == 0) {
        continue;
      }
      long lowerBound = i == firstBucket ? mins[i] : lowerBounds[i - 1];
      long upperBound = i == lastBucket ? maxs[i] : lowerBounds[i];
      out.printf("[%" + maxLowerWidth + "s .. %" + maxUpperWidth + "s%s  %" + maxCountWidth + "d (%6.2f%%)  %" + maxSumWidth + "s (%6.2f%%)\n",
          formatter.apply(lowerBound),
          formatter.apply(upperBound),
          i == lastBucket ? "]" : ")",
          counts[i], 100.0 * counts[i] / totalCount,
          formatter.apply(sums[i]), 100.0 * sums[i] / totalSum);
    }
    out.printf("min: %s  avg: %s  max: %s\n", formatter.apply(minValue), formatter.apply(totalSum / totalCount), formatter.apply(maxValue));
    out.printf("total count: %d  total sum: %s\n", totalCount, formatter.apply(totalSum));
  }

  private int findBucket(long value) {
    int res = Arrays.binarySearch(lowerBounds, value);
    if (res >= 0) {
      return res + 1;
    } else {
      return -(res + 1);
    }
  }

  public long sum() {
    return Arrays.stream(sums).sum();
  }
}

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

package ws.moor.gletscher.cloud;

import ws.moor.gletscher.util.ByteSize;

import java.io.PrintStream;
import java.util.Map;

public class CostTracker {

  // TODO(pmoor): use micros (int arithmetic)
  private static final double PRICE_PER_CLASS_A = 0.10 / 10_000;
  private static final double PRICE_PER_CLASS_B = 0.01 / 10_000;
  private static final double PRICE_PER_BYTE_RETRIEVED = (0.12 + 0.01) / (1 << 30);

  private long insertRequests = 0;
  private long insertSize = 0;
  private long listRequests = 0;
  private long headRequests = 0;
  private long getRequests = 0;
  private long getSize = 0;

  synchronized void trackInsert(Map<String, String> metadata, int length) {
    int totalSize = length;
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      totalSize += entry.getKey().length() + entry.getValue().length();
    }

    insertRequests += 1;
    insertSize += totalSize;
  }

  synchronized void trackList() {
    listRequests += 1;
  }

  synchronized void trackHead() {
    headRequests += 1;
  }

  synchronized void trackGet(int length) {
    getRequests += 1;
    getSize += length;
  }

  public synchronized void printSummary(PrintStream stream) {
    double totalCost = 0;

    if (insertRequests > 0) {
      double cost = PRICE_PER_CLASS_A * insertRequests;
      stream.printf("insert: %d, %s, $%02.6f\n", insertRequests, ByteSize.ofBytes(insertSize), cost);
      totalCost += cost;
    }

    if (getRequests > 0) {
      double cost = PRICE_PER_CLASS_B * getRequests + PRICE_PER_BYTE_RETRIEVED * getSize;
      stream.printf("get: %d, %s, $%02.6f\n", getRequests, ByteSize.ofBytes(getSize), cost);
      totalCost += cost;
    }

    if (headRequests > 0) {
      double cost = PRICE_PER_CLASS_B * headRequests;
      stream.printf("head: %d, $%02.6f\n", headRequests, cost);
      totalCost += cost;
    }

    if (listRequests > 0) {
      double cost = PRICE_PER_CLASS_A * listRequests;
      stream.printf("list: %d, $%02.6f\n", listRequests, cost);
      totalCost += cost;
    }

    if (totalCost > 0) {
      stream.printf("Total Cost (Approximate): $%02.6f\n", totalCost);
    }
  }

  public synchronized boolean hasUsage() {
    return insertRequests > 0 || listRequests > 0 || headRequests > 0 || getRequests > 0;
  }
}

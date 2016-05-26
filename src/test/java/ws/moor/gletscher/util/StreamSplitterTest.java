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

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.util.StreamSplitter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(JUnit4.class)
public class StreamSplitterTest {

  @Test
  public void findsDuplicatesAtRandomPlaces() throws IOException {
    Random rnd = new Random(0);

    for (int i = 0; i < 10; i++) {
      byte[] fileA = new byte[128 << 20];
      rnd.nextBytes(fileA);

      byte[] fileB = new byte[128 << 20];
      rnd.nextBytes(fileB);

      int offset = 0;
      int totalCommonSize = 0;
      while (offset < 120 << 20) {
        int subsetSize = rnd.nextInt(fileA.length - offset);
        int startOffset = offset + rnd.nextInt(fileA.length - subsetSize - offset);
        int targetOffset = offset + rnd.nextInt(fileB.length - subsetSize - offset);
        System.arraycopy(fileA, startOffset, fileB, targetOffset, subsetSize);
        offset = startOffset + subsetSize;
        totalCommonSize += subsetSize;
      }

      StreamSplitter splitter = StreamSplitter.rollingHashSplitter(48 << 20);

      Set<HashCode> fileAHashes = new HashSet<>();
      splitter.split(new ByteArrayInputStream(fileA)).forEachRemaining(block -> {
        fileAHashes.add(Hashing.md5().hashBytes(block));
      });

      AtomicInteger sharedSize = new AtomicInteger();
      splitter.split(new ByteArrayInputStream(fileB)).forEachRemaining(block -> {
        HashCode hashCode = Hashing.md5().hashBytes(block);
        if (fileAHashes.contains(hashCode)) {
          sharedSize.getAndAdd(block.length);
        }
      });
      System.out.printf("common MB: %.3f\tfound MB: %.3f\tefficiency: %.2f%%\n", totalCommonSize / 1024.0 / 1024.0, sharedSize.get() / 1024.0 / 1024.0, 100.0 * sharedSize.get() / totalCommonSize);
    }
  }
}

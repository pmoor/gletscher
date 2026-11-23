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

import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class StreamSplitterTest {

  @Test
  public void findsCommonBlocksIfOffsetByOne() throws IOException {
    Random rnd = new Random(0);

    byte[] fileA = MoreArrays.randomBytes(rnd, 4 << 20);
    byte[] fileB = Arrays.copyOfRange(fileA, 1, fileA.length);

    StreamSplitter splitter = StreamSplitter.rollingHashSplitter(32 << 20);

    Set<HashCode> fileAHashes = new HashSet<>();
    splitter
        .split(new ByteArrayInputStream(fileA))
        .forEachRemaining(block -> fileAHashes.add(Hashing.sha256().hashBytes(block)));

    Set<HashCode> fileBHashes = new HashSet<>();
    splitter
        .split(new ByteArrayInputStream(fileB))
        .forEachRemaining(block -> fileBHashes.add(Hashing.sha256().hashBytes(block)));

    assertThat(Sets.union(fileAHashes, fileBHashes).size())
        .isLessThan(fileAHashes.size() + fileBHashes.size());
  }

  @Test
  public void matchesReferenceRollingHash() {
    Random rnd = new Random(42);
    // 8 MB yields multiple rolling hash split points (expected block size ~4 MB)
    byte[] data = MoreArrays.randomBytes(rnd, 8 << 20);
    int maxBlockSize = 32 << 20;

    List<byte[]> reference = referenceSplit(data, maxBlockSize);
    assertThat(reference.size()).isGreaterThan(1);

    StreamSplitter splitter = StreamSplitter.rollingHashSplitter(maxBlockSize);
    List<byte[]> actual = collectBlocks(splitter.split(new ByteArrayInputStream(data)));
    assertBlocksEqual(actual, reference);
  }

  @Test
  public void partialReadsProduceIdenticalBlocks() {
    Random rnd = new Random(123);
    byte[] data = MoreArrays.randomBytes(rnd, 4 << 20);
    int maxBlockSize = 1 << 20;

    List<byte[]> reference = referenceSplit(data, maxBlockSize);

    int[] chunkSizes = new int[] {7, 13, 1024, 8191, 8192, 8193, 65536};
    for (int chunkSize : chunkSizes) {
      StreamSplitter splitter = StreamSplitter.rollingHashSplitter(maxBlockSize);
      InputStream chunked = new ChunkedInputStream(new ByteArrayInputStream(data), chunkSize);
      List<byte[]> actual = collectBlocks(splitter.split(chunked));
      assertBlocksEqual(actual, reference);
    }
  }

  @Test
  public void edgeCases() {
    Random rnd = new Random(7);
    byte[][] inputs =
        new byte[][] {
          new byte[0],
          new byte[] {42},
          MoreArrays.randomBytes(rnd, 100),
          MoreArrays.randomBytes(rnd, RollingHash.BUFFER_SIZE - 1),
          MoreArrays.randomBytes(rnd, RollingHash.BUFFER_SIZE),
          MoreArrays.randomBytes(rnd, RollingHash.BUFFER_SIZE + 1),
          MoreArrays.randomBytes(rnd, RollingHash.BUFFER_SIZE + 17),
          new byte[RollingHash.BUFFER_SIZE * 2 + 5], // all zeros
        };
    int[] maxBlockSizes = new int[] {1, 17, 1000, RollingHash.BUFFER_SIZE, 65536};

    for (byte[] data : inputs) {
      for (int maxBlockSize : maxBlockSizes) {
        if (maxBlockSize == 1 && data.length > 200) {
          continue;
        }
        List<byte[]> reference = referenceSplit(data, maxBlockSize);
        StreamSplitter splitter = StreamSplitter.rollingHashSplitter(maxBlockSize);
        List<byte[]> actual = collectBlocks(splitter.split(new ByteArrayInputStream(data)));
        assertBlocksEqual(actual, reference);
      }
    }
  }

  private static List<byte[]> referenceSplit(byte[] data, int maxBlockSize) {
    List<byte[]> blocks = new ArrayList<>();
    RollingHash hash = new RollingHash();
    ByteArrayOutputStream block = new ByteArrayOutputStream();
    for (byte b : data) {
      int value = b & 0xff;
      block.write(value);
      if (hash.update(value) || block.size() == maxBlockSize) {
        blocks.add(block.toByteArray());
        block.reset();
        hash.reset();
      }
    }
    if (block.size() > 0) {
      blocks.add(block.toByteArray());
    }
    return blocks;
  }

  private static List<byte[]> collectBlocks(Iterator<byte[]> it) {
    List<byte[]> blocks = new ArrayList<>();
    it.forEachRemaining(blocks::add);
    return blocks;
  }

  private static void assertBlocksEqual(List<byte[]> actual, List<byte[]> expected) {
    assertThat(actual.size()).isEqualTo(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.get(i)).isEqualTo(expected.get(i));
    }
  }

  private static final class ChunkedInputStream extends FilterInputStream {
    private final int maxChunk;

    ChunkedInputStream(InputStream in, int maxChunk) {
      super(in);
      this.maxChunk = maxChunk;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return super.read(b, off, Math.min(len, maxChunk));
    }
  }
}

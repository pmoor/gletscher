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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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
        .forEachRemaining(block -> fileAHashes.add(Hashing.md5().hashBytes(block)));

    Set<HashCode> fileBHashes = new HashSet<>();
    splitter
        .split(new ByteArrayInputStream(fileB))
        .forEachRemaining(block -> fileBHashes.add(Hashing.md5().hashBytes(block)));

    assertThat(Sets.union(fileAHashes, fileBHashes).size())
        .isLessThan(fileAHashes.size() + fileBHashes.size());
  }
}

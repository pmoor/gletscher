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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class RollingHashTest {

  @Test
  public void something() {
    Random rnd = new Random(1);

    RollingHash[] hashes = new RollingHash[128];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = new RollingHash();
      for (int j = 0; j < rnd.nextInt(RollingHash.BUFFER_SIZE); j++) {
        hashes[i].update(randomByte(rnd));
      }
    }

    for (int i = 0; i < hashes.length; i++) {
      for (int j = i + 1; j < hashes.length; j++) {
        assertThat(hashes[i]).isNotEqualTo(hashes[j]);
        assertThat(hashes[i].adler32()).isNotEqualTo(hashes[j].adler32());
      }
    }

    for (int i = 0; i < 2 * RollingHash.BUFFER_SIZE; i++) {
      for (RollingHash hash : hashes) {
        hash.update(randomByte(rnd));
      }
    }

    for (int i = 0; i < hashes.length; i++) {
      for (int j = i + 1; j < hashes.length; j++) {
        assertThat(hashes[i]).isNotEqualTo(hashes[j]);
        assertThat(hashes[i].adler32()).isNotEqualTo(hashes[j].adler32());
      }
    }

    for (int i = 32; i < RollingHash.BUFFER_SIZE; i++) {
      int rndByte = randomByte(rnd);
      for (RollingHash hash : hashes) {
        hash.update(rndByte);
      }
    }

    for (int i = 0; i < hashes.length; i++) {
      for (int j = i + 1; j < hashes.length; j++) {
        assertThat(hashes[i]).isNotEqualTo(hashes[j]);
        assertThat(hashes[i].adler32()).isNotEqualTo(hashes[j].adler32());
      }
    }

    for (int i = 0; i < 32; i++) {
      int rndByte = randomByte(rnd);
      for (RollingHash hash : hashes) {
        hash.update(rndByte);
      }
    }

    for (int i = 0; i < hashes.length; i++) {
      for (int j = i; j < hashes.length; j++) {
        assertThat(hashes[i]).isEqualTo(hashes[j]);
        assertThat(hashes[i].adler32()).isEqualTo(hashes[j].adler32());
      }
    }

    for (int k = 0; k < 4; k++) {
      for (int i = 0; i < RollingHash.BUFFER_SIZE / 2; i++) {
        int rndByte = randomByte(rnd);
        for (RollingHash hash : hashes) {
          hash.update(rndByte);
        }
      }

      for (int i = 0; i < hashes.length; i++) {
        for (int j = i; j < hashes.length; j++) {
          assertThat(hashes[i]).isEqualTo(hashes[j]);
          assertThat(hashes[i].adler32()).isEqualTo(hashes[j].adler32());
        }
      }
    }
  }

  private int randomByte(Random rnd) {
    return rnd.nextInt(256);
  }

  @Test
  public void knownValues() {
    RollingHash a = new RollingHash();

    assertThat(a.adler32()).isEqualTo(626934);

    for (int i = 0; i < 0xff; i++) {
      a.update(i);
    }

    assertThat(a.adler32()).isEqualTo(638243);
  }

  @Test
  public void runABunch() throws IOException {
    Random rnd = new Random();
    List<Long> runs = new ArrayList<>();
    long sum = 0;
    for (int i = 0; i < 100; i++) {
      RollingHash h = new RollingHash();
      long runLength = 1;
      while (!h.update(rnd.nextInt(0xff))) {
        runLength++;
      }
      runs.add(runLength);
      sum += runLength;
    }

    System.err.printf("avg: %d\n", sum / runs.size());
  }

  @Test
  public void lengthDistribution() {
    Random rnd = new Random(2);

    RollingHash a = new RollingHash();
    int min = Integer.MAX_VALUE;
    int max = 0;
    int greater16 = 0;
    int runs = 0;
    int runLength = 0;
    for (long i = 0; i < (1 << 30); i++) {
      runLength++;
      if (a.update(rnd.nextInt(0xff))) {
        runs++;
        min = Math.min(min, runLength);
        max = Math.max(max, runLength);
        if (runLength > (16 << 20)) {
          greater16++;
        }
        runLength = 0;
        a.reset();
      }
    }
    System.err.println(runs);
    System.err.println(greater16);
    System.err.println(min);
    System.err.println(max);
    System.err.println((4L << 30) / runs);
  }
}

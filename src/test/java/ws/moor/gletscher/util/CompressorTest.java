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

import java.util.Random;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class CompressorTest {

  @Test
  public void roundtrip() {
    Random rnd = new Random();
    Compressor compressor = new Compressor();

    for (int i = 0; i < 100; i++) {
      byte[] plaintext = new byte[rnd.nextInt(512 << 10)];
      rnd.nextBytes(plaintext);

      assertThat(compressor.decompress(compressor.compress(plaintext))).isEqualTo(plaintext);
    }
  }
}

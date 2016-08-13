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

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class ByteSizeTest {

  @Test
  public void someSamples() {
    assertEquals("0 B", 0);
    assertEquals("1 B", 1);
    assertEquals("1023 B", 1023);
    assertEquals("1.000 KB", 1024);
    assertEquals("1.001 KB", 1025);
    assertEquals("2.000 KB", 2048);
    assertEquals("3.000 MB", 3 << 20);
    assertEquals("4.000 GB", 4L << 30);
    assertEquals("5.000 TB", 5L << 40);
    assertEquals("6.000 PB", 6L << 50);
    assertEquals("7168.000 PB", 7L << 60);
  }

  private void assertEquals(String expected, long bytes) {
    assertThat(ByteSize.ofBytes(bytes).toString()).isEqualTo(expected);
  }
}

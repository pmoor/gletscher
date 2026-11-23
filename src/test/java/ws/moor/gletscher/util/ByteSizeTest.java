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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(JUnit4.class)
public class ByteSizeTest {

  @Test
  public void someSamples() {
    assertEquals("0 B", 0);
    assertEquals("1 B", 1);
    assertEquals("1023 B", 1023);
    assertEquals("1.000 KiB", 1024);
    assertEquals("1.001 KiB", 1025);
    assertEquals("2.000 KiB", 2048);
    assertEquals("3.000 MiB", 3 << 20);
    assertEquals("4.000 GiB", 4L << 30);
    assertEquals("5.000 TiB", 5L << 40);
    assertEquals("6.000 PiB", 6L << 50);
    assertEquals("7.000 EiB", 7L << 60);
  }

  @Test
  public void parseSizes() {
    assertThat(ByteSize.parse("0").toBytes()).isEqualTo(0L);
    assertThat(ByteSize.parse(" 42 B ").toBytes()).isEqualTo(42L);
    assertThat(ByteSize.parse("2K").toBytes()).isEqualTo(2L << 10);
    assertThat(ByteSize.parse("2KiB").toBytes()).isEqualTo(2L << 10);
    assertThat(ByteSize.parse("2kb").toBytes()).isEqualTo(2_000L);
    assertThat(ByteSize.parse("3m").toBytes()).isEqualTo(3L << 20);
    assertThat(ByteSize.parse("3 MiB").toBytes()).isEqualTo(3L << 20);
    assertThat(ByteSize.parse("3MB").toBytes()).isEqualTo(3_000_000L);
    assertThat(ByteSize.parse("4g").toBytes()).isEqualTo(4L << 30);
    assertThat(ByteSize.parse("4GiB").toBytes()).isEqualTo(4L << 30);
    assertThat(ByteSize.parse("4GB").toBytes()).isEqualTo(4_000_000_000L);
    assertThat(ByteSize.parse("5T").toBytes()).isEqualTo(5L << 40);
    assertThat(ByteSize.parse("5TiB").toBytes()).isEqualTo(5L << 40);
    assertThat(ByteSize.parse("5TB").toBytes()).isEqualTo(5_000_000_000_000L);
  }

  @Test
  public void parseInvalidSizes() {
    assertThrows(IllegalArgumentException.class, () -> ByteSize.parse(""));
    assertThrows(IllegalArgumentException.class, () -> ByteSize.parse("-10"));
    assertThrows(IllegalArgumentException.class, () -> ByteSize.parse("10X"));
    assertThrows(IllegalArgumentException.class, () -> ByteSize.parse("abc"));
    assertThrows(IllegalArgumentException.class, () -> ByteSize.parse("9999999999T"));
  }

  private void assertEquals(String expected, long bytes) {
    assertThat(ByteSize.ofBytes(bytes).toString()).isEqualTo(expected);
  }
}

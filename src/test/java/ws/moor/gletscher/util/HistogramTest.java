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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class HistogramTest {
  @Test
  public void test() {
    Histogram h = new Histogram(2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);
    for (long i = 0; i < 8192; i+=5) {
      h.add(i);
    }

    String output = printToString(h, bytesFormatter());
    assertThat(output).isEqualTo("""
                           counts                   sums
    [      0 B ..       2 B)    1 (  0.06%)          0 B (  0.00%)
    [      4 B ..       8 B)    1 (  0.06%)          5 B (  0.00%)
    [      8 B ..      16 B)    2 (  0.12%)         25 B (  0.00%)
    [     16 B ..      32 B)    3 (  0.18%)         75 B (  0.00%)
    [     32 B ..      64 B)    6 (  0.37%)        285 B (  0.00%)
    [     64 B ..     128 B)   13 (  0.79%)    1.206 KiB (  0.02%)
    [    128 B ..     256 B)   26 (  1.59%)    4.888 KiB (  0.07%)
    [    256 B ..     512 B)   51 (  3.11%)   19.175 KiB (  0.29%)
    [    512 B .. 1.000 KiB)  102 (  6.22%)   76.450 KiB (  1.17%)
    [1.000 KiB .. 2.000 KiB)  205 ( 12.51%)  307.300 KiB (  4.69%)
    [2.000 KiB .. 4.000 KiB)  410 ( 25.02%)    1.201 MiB ( 18.77%)
    [4.000 KiB .. 7.998 KiB]  819 ( 49.97%)    4.800 MiB ( 74.98%)
    min: 0 B  avg: 3.999 KiB  max: 7.998 KiB
    total count: 1639  total sum: 6.401 MiB
    """);
  }

  @Test
  public void noBounds() {
    Histogram h = new Histogram();
    h.add(-15);
    h.add(0);
    h.add(30);

    String output = printToString(h, String::valueOf);
    assertThat(output).isEqualTo("""
            counts          sums
    [-15 .. 30]  3 (100.00%)  15 (100.00%)
    min: -15  avg: 5  max: 30
    total count: 3  total sum: 15
    """);
  }

  @Test
  public void empty() {
    Histogram h = new Histogram();

    String output = printToString(h, bytesFormatter());
    assertThat(output).isEqualTo("""
    total count: 0  total sum: 0 B
    """);
  }

  private static String printToString(Histogram h, Function<Long, String> formatter) {
    StringWriter writer = new StringWriter();
    h.write(formatter, new PrintWriter(writer));
    return writer.toString();
  }

  private static Function<Long, String> bytesFormatter() {
    return s -> ByteSize.ofBytes(s).toString();
  }
}

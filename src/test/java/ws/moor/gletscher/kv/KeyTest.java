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

package ws.moor.gletscher.kv;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class KeyTest {

  @Test
  public void minMax() {
    assertEqual(Key.MIN, Key.MIN);
    assertEqual(Key.MAX, Key.MAX);

    assertThat(Key.MIN).isLessThan(Key.MAX);
    assertThat(Key.MAX).isGreaterThan(Key.MIN);
  }

  @Test
  public void basics() {
    assertEqual(Key.fromHex("00"), Key.copyOf(new byte[] {0}));
    assertEqual(Key.fromHex("ffff"), Key.copyOf(new byte[] {(byte) 0xff, (byte) 0xff}));
  }

  @Test
  public void between() {
    Key a = Key.fromHex("00");
    Key b = Key.fromHex("0102");
    assertThat(a.findBetween(b)).isEqualTo(Key.fromHex("01"));
  }

  @Test
  public void prefix() {
    assertEqual(Key.fromHex("5678").prefix(1), Key.fromHex("56"));
    assertEqual(Key.fromHex("1234").prefix(2), Key.fromHex("1234"));
  }

  private static void assertEqual(Key a, Key b) {
    assertThat(a).isEqualTo(b);
    assertThat(a).isEquivalentAccordingToCompareTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a.toString()).isEqualTo(b.toString());
  }
}

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

package ws.moor.gletscher.kv;

import com.google.common.collect.Iterators;
import com.google.common.jimfs.Jimfs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.util.MoreArrays;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class KVStoreRandomizedIT {

  @Test
  public void oneMillionKeys() throws Exception {
    FileSystem fs = Jimfs.newFileSystem();
    Path root = fs.getPath("/tmp/kv-store");
    Files.createDirectories(root);

    Random rnd = new Random(567);
    int index = 0;

    while (index < 1_000_000) {
      int num = rnd.nextInt(100_000) + 1;
      KVStore store = KVStores.open(root);
      for (int i = 0; i < num; i++) {
        Key key = Key.fromUtf8(Integer.toHexString(Integer.reverse(index++)));
        byte[] value = new byte[rnd.nextInt(64)];
        rnd.nextBytes(value);
        store.store(key, value);
      }
      store.close();
    }

    for (int i = 0; i < 10; i++) {
      KVStore store = KVStores.open(root);
      Key key = Key.fromUtf8("hi");
      store.store(key, new byte[1]);
      store.close();
    }

    KVStore store = KVStores.open(root);
    for (int i = 0; i < 1_000_000; i++) {
      Key key = Key.fromUtf8(Integer.toHexString(Integer.reverse(i)));
      assertThat(store.get(key)).isNotNull();
    }
  }

  @Test
  public void randomized() throws Exception {
    FileSystem fs = Jimfs.newFileSystem();
    Path root = fs.getPath("/tmp/kv-store");
    Files.createDirectories(root);

    Random rnd = new Random(567);
    Set<Key> allKeysEver = new HashSet<>();
    List<Key> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    List<Key> deletedKeys = new ArrayList<>();

    int rounds = 50;
    int steps = 25_000;
    for (int round = 0; round < rounds; round++) {
      KVStore store = KVStores.open(root);

      int flushStep = rnd.nextInt(steps * 5);
      double activeKeysTarget = Math.max(0, 100_000d * (1 - (double) round / 1.1 / rounds));
      for (int i = 0; i < steps; i++) {
        if (odds(rnd, 1.0 - allKeysEver.size() / 200_000d)) {
          // add a key
          Key key = pickUniqueKey(rnd, allKeysEver);
          allKeysEver.add(key);

          byte[] value = MoreArrays.randomBytes(rnd, rnd.nextInt(512));
          debugKey("adding key", key);
          store.store(key, value);
          keys.add(key);
          values.add(value);
        }
        if (keys.size() > 0 && odds(rnd, keys.size() / 2 / activeKeysTarget)) {
          // remove a key
          int index = rnd.nextInt(keys.size());
          Key key = keys.get(index);
          debugKey("deleting key", key);
          store.delete(key);

          keys.set(index, keys.get(keys.size() - 1));
          values.set(index, values.get(values.size() - 1));
          keys.remove(keys.size() - 1);
          values.remove(values.size() - 1);
          deletedKeys.add(key);
        }
        if (keys.size() > 0 && odds(rnd, 0.2)) {
          // overwrite a key
          int index = rnd.nextInt(keys.size());
          Key key = keys.get(index);
          debugKey("overwriting key", key);
          byte[] value = MoreArrays.randomBytes(rnd, rnd.nextInt(512));
          store.store(key, value);

          values.set(index, value);
        }
        if (deletedKeys.size() > 0 && odds(rnd, 1.0 - keys.size() / 2 / activeKeysTarget)) {
          // re-surrect a deleted key
          int index = rnd.nextInt(deletedKeys.size());
          Key key = deletedKeys.get(index);
          debugKey("resurrecting key", key);
          byte[] value = MoreArrays.randomBytes(rnd, rnd.nextInt(512));
          store.store(key, value);

          deletedKeys.set(index, deletedKeys.get(deletedKeys.size() - 1));
          deletedKeys.remove(deletedKeys.size() - 1);
          keys.add(key);
          values.add(value);
        }
        if (keys.size() > 0 && rnd.nextBoolean()) {
          // check a random key
          int index = rnd.nextInt(keys.size());
          Key key = keys.get(index);
          byte[] value = values.get(index);

          if (rnd.nextBoolean()) {
            debugKey("checking existing key contains", key);
            assertThat(store.contains(key)).isTrue();
          } else {
            debugKey("checking existing key get", key);
            assertThat(store.get(key)).isEqualTo(value);
          }
        }
        if (deletedKeys.size() > 0 && rnd.nextBoolean()) {
          // check a random deleted key
          int index = rnd.nextInt(deletedKeys.size());
          Key key = deletedKeys.get(index);

          if (rnd.nextBoolean()) {
            debugKey("checking deleted key contains", key);
            assertThat(store.contains(key)).isFalse();
          } else {
            debugKey("checking deleted key get", key);
            assertThat(store.get(key)).isNull();
          }
        }
        if (rnd.nextBoolean()) {
          // lookup a random, non-existing key
          Key key = pickUniqueKey(rnd, allKeysEver);
          if (rnd.nextBoolean()) {
            debugKey("checking new key contains", key);
            assertThat(store.contains(key)).isFalse();
          } else {
            debugKey("checking new key get", key);
            assertThat(store.get(key)).isNull();
          }
        }
        if (odds(rnd, 0.0001)) {
          // iterate over all keys - expensive
          NavigableMap<Key, KVStore.Entry> foundKeys = new TreeMap<>();
          Key startKey = rnd.nextBoolean() ? Key.MIN : Key.MAX;
          if (rnd.nextBoolean() && !keys.isEmpty()) {
            startKey = keys.get(rnd.nextInt(keys.size()));
          } else if (rnd.nextBoolean() && !deletedKeys.isEmpty()) {
            startKey = deletedKeys.get(rnd.nextInt(deletedKeys.size()));
          } else if (rnd.nextBoolean()) {
            startKey = pickUniqueKey(rnd, allKeysEver);
          }

          boolean inclusive = rnd.nextBoolean();
          boolean ascending = rnd.nextBoolean();
          Iterator<KVStore.Entry> iterator = store.iterator(startKey, inclusive, ascending);
          while (iterator.hasNext()) {
            KVStore.Entry entry = iterator.next();
            foundKeys.put(entry.getKey(), entry);
            if (ascending) {
              assertThat(foundKeys.lastKey()).isEqualTo(entry.getKey());
            } else {
              assertThat(foundKeys.firstKey()).isEqualTo(entry.getKey());
            }
          }

          for (int j = 0; j < keys.size(); j++) {
            Key key = keys.get(j);
            if (keyShouldBeReturned(startKey, key, inclusive, ascending)) {
              assertThat(foundKeys.containsKey(key)).isTrue();
              assertThat(foundKeys.get(key).size()).isEqualTo(values.get(j).length);
              assertThat(foundKeys.get(key).read()).isEqualTo(values.get(j));
            } else {
              assertThat(foundKeys.containsKey(key)).isFalse();
            }
          }
          for (Key key : deletedKeys) {
            assertThat(foundKeys).doesNotContainKey(key);
          }
        }
        if (i == flushStep) {
          store.flush();
        }
      }

      store.close();
    }

    KVStore store = KVStores.open(root);
    assertThat(Iterators.size(store.iterator(Key.MAX, true, false))).isEqualTo(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      assertThat(store.contains(keys.get(i))).isTrue();
      assertThat(store.get(keys.get(i))).isEqualTo(values.get(i));
      store.delete(keys.get(i));
    }
    for (Key key : allKeysEver) {
      assertThat(store.contains(key)).isFalse();
      assertThat(store.get(key)).isNull();
    }
    assertThat(Iterators.size(store.iterator(Key.MIN, true, true))).isEqualTo(0);
    store.close();
  }

  private boolean keyShouldBeReturned(Key startKey, Key key, boolean inclusive, boolean ascending) {
    int cmp = startKey.compareTo(key);
    if (ascending) {
      return cmp < 0 || (cmp == 0 && inclusive);
    } else {
      return cmp > 0 || (cmp == 0 && inclusive);
    }
  }

  private boolean odds(Random rnd, double odds) {
    return rnd.nextDouble() < odds;
  }

  private void debugKey(String message, Key key) {
    if (key.equals(Key.fromHex("43356c1b9a546d00a3edc6fcbb468d93acf0"))) {
      System.err.printf("%s: %s\n", key, message);
    }
  }

  private Key pickUniqueKey(Random rnd, Set<Key> allKeysEver) {
    Key key;
    do {
      key = Key.copyOf(MoreArrays.randomBytes(rnd, rnd.nextInt(64) + 1));
    } while (allKeysEver.contains(key));
    return key;
  }
}

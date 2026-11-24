/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.cloud;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.util.LegacyHashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(JUnit4.class)
public class PretendMissingCloudFileStorageTest {

  private InMemoryCloudFileStorage backing;

  @Before
  public void setUp() throws Exception {
    backing = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    store("backups/1", "b1");
    store("backups/2", "b2");
    store("backups/3", "b3");
    store("blocks/00/aa", "d1");
    store("blocks/00/bb", "d2");
  }

  @Test
  public void hidesConfiguredObjectsAcrossExistsGetAndListFiles() throws Exception {
    PretendMissingCloudFileStorage storage =
        new PretendMissingCloudFileStorage(backing, Set.of("backups/1", "blocks/00/bb"));

    assertThat(storage.exists("backups/1").get()).isFalse();
    assertThat(storage.exists("backups/2").get()).isTrue();
    assertThat(storage.exists("blocks/00/bb").get()).isFalse();
    assertThat(storage.exists("blocks/00/aa").get()).isTrue();

    assertThat(storage.get("backups/1").get()).isNull();
    assertThat(storage.get("backups/2").get()).isEqualTo("b2".getBytes(StandardCharsets.UTF_8));
    assertThat(storage.get("blocks/00/bb").get()).isNull();
    assertThat(storage.get("blocks/00/aa").get()).isEqualTo("d1".getBytes(StandardCharsets.UTF_8));

    assertThat(listNames(storage, "backups/", Integer.MAX_VALUE))
        .containsExactly("backups/2", "backups/3")
        .inOrder();
    assertThat(listNames(storage, "backups/", 1)).containsExactly("backups/2");
    assertThat(listNames(storage, "backups/", 2))
        .containsExactly("backups/2", "backups/3")
        .inOrder();
    assertThat(listNames(storage, "backups/", 0)).isEmpty();
    assertThat(listNames(storage, "blocks/", Integer.MAX_VALUE)).containsExactly("blocks/00/aa");
  }

  @Test
  public void listFilesRequestsBoundedLimitFromDelegate() {
    List<Integer> observedLimits = new ArrayList<>();
    CloudFileStorage recordingDelegate =
        new CloudFileStorage() {
          @Override
          public com.google.common.util.concurrent.ListenableFuture<?> store(
              String name,
              byte[] data,
              com.google.common.hash.HashCode md5,
              java.util.Map<String, String> metadata,
              StoreOptions options) {
            return backing.store(name, data, md5, metadata, options);
          }

          @Override
          public Iterator<FileHeader> listFiles(String prefix, int limit) {
            observedLimits.add(limit);
            return backing.listFiles(prefix, limit);
          }

          @Override
          public com.google.common.util.concurrent.ListenableFuture<Boolean> exists(String name) {
            return backing.exists(name);
          }

          @Override
          public com.google.common.util.concurrent.ListenableFuture<byte[]> get(
              String name, GetOptions options) {
            return backing.get(name, options);
          }

          @Override
          public void close() {
            backing.close();
          }
        };

    PretendMissingCloudFileStorage storage =
        new PretendMissingCloudFileStorage(
            recordingDelegate, Set.of("backups/1", "blocks/00/bb"));

    assertThat(listNames(storage, "backups/", 1)).containsExactly("backups/2");
    assertThat(listNames(storage, "backups/", Integer.MAX_VALUE))
        .containsExactly("backups/2", "backups/3")
        .inOrder();
    assertThat(observedLimits).containsExactly(2, Integer.MAX_VALUE).inOrder();
  }

  @Test
  public void storeIsUnsupported() {
    PretendMissingCloudFileStorage storage =
        new PretendMissingCloudFileStorage(backing, Set.of());
    byte[] data = "x".getBytes(StandardCharsets.UTF_8);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            storage.store(
                "blocks/00/cc",
                data,
                LegacyHashing.md5().hashBytes(data),
                ImmutableMap.of(),
                CloudFileStorage.StoreOptions.DEFAULT));
  }

  private void store(String name, String content) throws Exception {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    backing
        .store(
            name,
            bytes,
            LegacyHashing.md5().hashBytes(bytes),
            ImmutableMap.of(),
            CloudFileStorage.StoreOptions.DEFAULT)
        .get();
  }

  private static List<String> listNames(CloudFileStorage storage, String prefix, int limit) {
    List<String> names = new ArrayList<>();
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles(prefix, limit);
    while (it.hasNext()) {
      names.add(it.next().name);
    }
    return names;
  }
}

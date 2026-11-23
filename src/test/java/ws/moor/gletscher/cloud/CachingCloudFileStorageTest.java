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
import com.google.common.hash.HashCode;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.util.LegacyHashing;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class CachingCloudFileStorageTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testPersistsDataAndExistenceAcrossReopen() throws Exception {
    Path cacheDir = tempFolder.newFolder("cache").toPath();
    InMemoryCloudFileStorage backing =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    CountingDelegate delegate = new CountingDelegate(backing);

    byte[] cachedBytes = "cached-data".getBytes(StandardCharsets.UTF_8);
    HashCode cachedMd5 = LegacyHashing.md5().hashBytes(cachedBytes);
    byte[] uncachedBytes = "uncached-data".getBytes(StandardCharsets.UTF_8);
    HashCode uncachedMd5 = LegacyHashing.md5().hashBytes(uncachedBytes);

    try (CachingCloudFileStorage cache = new CachingCloudFileStorage(delegate, cacheDir)) {
      cache
          .store(
              "meta/1",
              cachedBytes,
              cachedMd5,
              ImmutableMap.of(),
              CloudFileStorage.StoreOptions.builder().setCacheContentsOnUpload(true).build())
          .get();
      cache
          .store(
              "data/1",
              uncachedBytes,
              uncachedMd5,
              ImmutableMap.of(),
              CloudFileStorage.StoreOptions.DEFAULT)
          .get();

      assertThat(cache.exists("meta/1").get()).isTrue();
      assertThat(cache.get("meta/1").get()).isEqualTo(cachedBytes);
      assertThat(delegate.existsCalls.get()).isEqualTo(0);
      assertThat(delegate.getCalls.get()).isEqualTo(0);

      // data/1 was not cached on upload, so first exists and get hit the delegate once.
      assertThat(cache.exists("data/1").get()).isTrue();
      assertThat(cache.get("data/1").get()).isEqualTo(uncachedBytes);
      assertThat(delegate.existsCalls.get()).isEqualTo(1);
      assertThat(delegate.getCalls.get()).isEqualTo(1);
    }

    assertThat(Files.exists(cacheDir.resolve("cache.mv"))).isTrue();

    // Reopen on the same directory and verify hits do not reach the delegate.
    try (CachingCloudFileStorage reopened = new CachingCloudFileStorage(delegate, cacheDir)) {
      assertThat(reopened.exists("meta/1").get()).isTrue();
      assertThat(reopened.get("meta/1").get()).isEqualTo(cachedBytes);
      assertThat(reopened.exists("data/1").get()).isTrue();
      assertThat(reopened.get("data/1").get()).isEqualTo(uncachedBytes);
      assertThat(delegate.existsCalls.get()).isEqualTo(1);
      assertThat(delegate.getCalls.get()).isEqualTo(1);
    }
  }

  @Test
  public void testNonDefaultFileSystemUsesInMemoryStore() throws Exception {
    try (FileSystem fs = Jimfs.newFileSystem(Configuration.unix())) {
      Path cacheDir = fs.getPath("/cache");
      Files.createDirectories(cacheDir);
      InMemoryCloudFileStorage backing =
          new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
      CountingDelegate delegate = new CountingDelegate(backing);

      byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
      HashCode md5 = LegacyHashing.md5().hashBytes(bytes);

      try (CachingCloudFileStorage cache = new CachingCloudFileStorage(delegate, cacheDir)) {
        cache
            .store(
                "item/1",
                bytes,
                md5,
                ImmutableMap.of(),
                CloudFileStorage.StoreOptions.builder().setCacheContentsOnUpload(true).build())
            .get();

        assertThat(cache.exists("item/1").get()).isTrue();
        assertThat(cache.get("item/1").get()).isEqualTo(bytes);
        assertThat(delegate.existsCalls.get()).isEqualTo(0);
        assertThat(delegate.getCalls.get()).isEqualTo(0);
      }
    }
  }

  @Test
  public void testCachesUnlimitedListingsAcrossReopenAndRespectsMaxAge() throws Exception {
    Path cacheDir = tempFolder.newFolder("listing-cache").toPath();
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-06-01T00:00:00Z"));
    Clock clock =
        new Clock() {
          @Override
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return now.get();
          }
        };

    InMemoryCloudFileStorage backing =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService(), clock);
    CountingDelegate delegate = new CountingDelegate(backing);

    byte[] payload1 = "block-1".getBytes(StandardCharsets.UTF_8);
    byte[] payload2 = "block-2-longer".getBytes(StandardCharsets.UTF_8);
    backing
        .store(
            "blocks/aa/1",
            payload1,
            LegacyHashing.md5().hashBytes(payload1),
            ImmutableMap.of("sig", "s1"),
            CloudFileStorage.StoreOptions.DEFAULT)
        .get();
    backing
        .store(
            "blocks/bb/2",
            payload2,
            LegacyHashing.md5().hashBytes(payload2),
            ImmutableMap.of("sig", "s2"),
            CloudFileStorage.StoreOptions.DEFAULT)
        .get();
    backing.setStorageClass("blocks/aa/1", "COLDLINE");
    backing.setCreatedTime("blocks/aa/1", Instant.parse("2025-01-01T00:00:00Z"));
    backing.setStorageClass("blocks/bb/2", "MULTI_REGIONAL");

    // Initial live listing with maxListingAge = ZERO writes to cache.mv.
    try (CachingCloudFileStorage cache =
        new CachingCloudFileStorage(delegate, cacheDir, Duration.ZERO, clock)) {
      List<CloudFileStorage.FileHeader> headers = listAll(cache, "blocks/", Integer.MAX_VALUE);
      assertThat(headers).hasSize(2);
      assertThat(delegate.listCalls.get()).isEqualTo(1);
    }

    // Mutate backing storage after the snapshot was cached.
    now.set(Instant.parse("2026-06-01T00:30:00Z")); // +30m
    byte[] payload3 = "block-3".getBytes(StandardCharsets.UTF_8);
    backing
        .store(
            "blocks/cc/3",
            payload3,
            LegacyHashing.md5().hashBytes(payload3),
            ImmutableMap.of(),
            CloudFileStorage.StoreOptions.DEFAULT)
        .get();

    // With maxListingAge = 1h, unlimited listing reads from cache (0 delegate calls).
    try (CachingCloudFileStorage cache =
        new CachingCloudFileStorage(delegate, cacheDir, Duration.ofHours(1), clock)) {
      List<CloudFileStorage.FileHeader> cachedHeaders = listAll(cache, "blocks/", Integer.MAX_VALUE);
      assertThat(cachedHeaders).hasSize(2);
      assertThat(cachedHeaders.get(0).storageClass).isEqualTo("COLDLINE");
      assertThat(cachedHeaders.get(1).storageClass).isEqualTo("MULTI_REGIONAL");
      assertThat(cachedHeaders.get(0).createdTime)
          .isEqualTo(Instant.parse("2025-01-01T00:00:00Z"));
      assertThat(cachedHeaders.get(0).metadata).containsExactly("sig", "s1");
      assertThat(delegate.listCalls.get()).isEqualTo(1);

      // Bounded listing bypasses the cache and hits the delegate.
      List<CloudFileStorage.FileHeader> bounded = listAll(cache, "blocks/", 1);
      assertThat(bounded).hasSize(1);
      assertThat(delegate.listCalls.get()).isEqualTo(2);

      // Partial unlimited iteration (not exhausted) must not overwrite the cached entry.
      now.set(Instant.parse("2026-06-01T01:05:00Z")); // expired (> 1h)
      Iterator<CloudFileStorage.FileHeader> partial = cache.listFiles("blocks/", Integer.MAX_VALUE);
      assertThat(partial.hasNext()).isTrue();
      partial.next();
      assertThat(delegate.listCalls.get()).isEqualTo(3);

      // Because partial didn't finish, previous entry is still older -> full call still refreshes.
      List<CloudFileStorage.FileHeader> refreshed = listAll(cache, "blocks/", Integer.MAX_VALUE);
      assertThat(refreshed).hasSize(3);
      assertThat(delegate.listCalls.get()).isEqualTo(4);

      // Calling again right away (now at 1h05m, within 1h of refresh) hits cache cleanly!
      List<CloudFileStorage.FileHeader> cachedAfterRefresh =
          listAll(cache, "blocks/", Integer.MAX_VALUE);
      assertThat(cachedAfterRefresh).hasSize(3);
      assertThat(delegate.listCalls.get()).isEqualTo(4);
    }
  }

  private static List<CloudFileStorage.FileHeader> listAll(
      CloudFileStorage storage, String prefix, int limit) {
    List<CloudFileStorage.FileHeader> result = new ArrayList<>();
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles(prefix, limit);
    while (it.hasNext()) {
      result.add(it.next());
    }
    return result;
  }

  private static final class CountingDelegate implements CloudFileStorage {
    private final CloudFileStorage delegate;
    final AtomicInteger existsCalls = new AtomicInteger();
    final AtomicInteger getCalls = new AtomicInteger();
    final AtomicInteger listCalls = new AtomicInteger();

    CountingDelegate(CloudFileStorage delegate) {
      this.delegate = delegate;
    }

    @Override
    public ListenableFuture<?> store(
        String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
      return delegate.store(name, data, md5, metadata, options);
    }

    @Override
    public Iterator<FileHeader> listFiles(String prefix, int limit) {
      listCalls.incrementAndGet();
      return delegate.listFiles(prefix, limit);
    }

    @Override
    public ListenableFuture<Boolean> exists(String name) {
      existsCalls.incrementAndGet();
      return delegate.exists(name);
    }

    @Override
    public ListenableFuture<byte[]> get(String name) {
      getCalls.incrementAndGet();
      return Futures.transform(
          delegate.get(name), bytes -> bytes, MoreExecutors.directExecutor());
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}

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

package ws.moor.gletscher.cloud;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import ws.moor.gletscher.kv.KVStore;
import ws.moor.gletscher.kv.KVStores;
import ws.moor.gletscher.kv.Key;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CachingCloudFileStorage implements CloudFileStorage {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final CloudFileStorage delegate;
  private final KVStore kvStore;
  private final Clock clock;
  private final ListeningExecutorService executor;
  private final BloomFilter<String> bloomFilter;

  public CachingCloudFileStorage(CloudFileStorage delegate, Path localCacheDir, Clock clock) {
    Preconditions.checkArgument(Files.isDirectory(localCacheDir, LinkOption.NOFOLLOW_LINKS));
    this.delegate = delegate;
    this.kvStore = KVStores.open(localCacheDir);
    this.clock = clock;
    this.executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cache-thread-%d").build()));

    this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 100_000);
  }

  @Override
  public ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    ListenableFuture<?> future = delegate.store(name, data, md5, metadata, options);
    if (options.cacheContentsOnUpload) {
      Futures.addCallback(future, new FutureCallback<Object>() {
        @Override public void onSuccess(@Nullable Object unused) {
          storeData(name, data);
        }
        @Override public void onFailure(Throwable throwable) { }
      }, executor);
    }
    return future;
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    return delegate.listFiles(prefix, limit);
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    ListenableFuture<Boolean> future = executor.submit(() -> checkExists(name));
    return Futures.transformAsync(future, input -> {
      if (input) {
        return Futures.immediateFuture(true);
      } else {
        ListenableFuture<Boolean> delegateFuture = delegate.exists(name);
        Futures.addCallback(delegateFuture, new FutureCallback<Boolean>() {
          @Override public void onSuccess(@Nullable Boolean exists) {
            if (exists) {
              storeExists(name);
            }
          }
          @Override public void onFailure(Throwable throwable) { }
        }, executor);
        return delegateFuture;
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    ListenableFuture<byte[]> data = executor.submit(() -> readData(name));
    return Futures.transformAsync(data, bytes -> {
      if (bytes != null) {
        return Futures.immediateFuture(bytes);
      } else {
        ListenableFuture<byte[]> delegateFuture = delegate.get(name);
        Futures.addCallback(delegateFuture, new FutureCallback<byte[]>() {
          @Override public void onSuccess(@Nullable byte[] data) {
            if (data != null) {
              storeData(name, data);
            }
          }
          @Override public void onFailure(Throwable t) { }
        }, executor);
        return delegateFuture;
      }
    }, MoreExecutors.directExecutor());
  }

  private void storeExists(String name) {
    bloomFilter.put(name);
    kvStore.store(Key.fromUtf8("e:" + name), EMPTY_BYTE_ARRAY);
  }

  private boolean checkExists(String name) {
    boolean exists = kvStore.contains(Key.fromUtf8("e:" + name))
        || kvStore.contains(Key.fromUtf8("d:" + name));
    if (exists) {
      bloomFilter.put(name);
    }
    return exists;
  }

  private void storeData(String name, byte[] data) {
    bloomFilter.put(name);
    kvStore.store(Key.fromUtf8("d:" + name), data);
  }

  private byte[] readData(String name) {
    byte[] data = kvStore.get(Key.fromUtf8("d:" + name));
    if (data != null) {
      bloomFilter.put(name);
    }
    return data;
  }

  @Override
  public void close() {
    delegate.close();
    MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.MINUTES);

    storeBloomFilter();
    kvStore.close();
  }

  private void storeBloomFilter() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      bloomFilter.writeTo(baos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Key key = Key.fromUtf8(String.format("bloom:%d", clock.millis()));
    kvStore.store(key, baos.toByteArray());
  }
}

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

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.ByteArrayDataType;
import org.h2.mvstore.type.StringDataType;

import javax.annotation.Nullable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CachingCloudFileStorage implements CloudFileStorage {

  private static final String CACHE_FILE_NAME = "cache.mv";
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private static final MVMap.Builder<String, byte[]> MAP_BUILDER =
      new MVMap.Builder<String, byte[]>()
          .keyType(StringDataType.INSTANCE)
          .valueType(ByteArrayDataType.INSTANCE);

  private final CloudFileStorage delegate;
  private final MVStore mvStore;
  private final MVMap<String, byte[]> existsMap;
  private final MVMap<String, byte[]> dataMap;
  private final ListeningExecutorService executor;

  public CachingCloudFileStorage(CloudFileStorage delegate, Path localCacheDir) {
    Preconditions.checkArgument(Files.isDirectory(localCacheDir, LinkOption.NOFOLLOW_LINKS));
    this.delegate = delegate;
    this.mvStore = openStore(localCacheDir);
    this.existsMap = mvStore.openMap("exists", MAP_BUILDER);
    this.dataMap = mvStore.openMap("data", MAP_BUILDER);
    this.executor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                1, new ThreadFactoryBuilder().setNameFormat("cache-thread-%d").build()));
  }

  private static MVStore openStore(Path localCacheDir) {
    MVStore.Builder builder = new MVStore.Builder();
    if (localCacheDir.getFileSystem().equals(FileSystems.getDefault())) {
      builder.fileName(localCacheDir.resolve(CACHE_FILE_NAME).toString());
    }
    return builder.open();
  }

  @Override
  public ListenableFuture<?> store(
      String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    ListenableFuture<?> future = delegate.store(name, data, md5, metadata, options);
    if (options.cacheContentsOnUpload) {
      Futures.addCallback(
          future,
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object unused) {
              storeData(name, data);
            }

            @Override
            public void onFailure(Throwable throwable) {}
          },
          executor);
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
    return Futures.transformAsync(
        future,
        input -> {
          if (Boolean.TRUE.equals(input)) {
            return Futures.immediateFuture(true);
          } else {
            ListenableFuture<Boolean> delegateFuture = delegate.exists(name);
            Futures.addCallback(
                delegateFuture,
                new FutureCallback<Boolean>() {
                  @Override
                  public void onSuccess(@Nullable Boolean exists) {
                    if (Boolean.TRUE.equals(exists)) {
                      storeExists(name);
                    }
                  }

                  @Override
                  public void onFailure(Throwable throwable) {}
                },
                executor);
            return delegateFuture;
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    ListenableFuture<byte[]> data = executor.submit(() -> readData(name));
    return Futures.transformAsync(
        data,
        bytes -> {
          if (bytes != null) {
            return Futures.immediateFuture(bytes);
          } else {
            ListenableFuture<byte[]> delegateFuture = delegate.get(name);
            Futures.addCallback(
                delegateFuture,
                new FutureCallback<byte[]>() {
                  @Override
                  public void onSuccess(@Nullable byte[] data) {
                    if (data != null) {
                      storeData(name, data);
                    }
                  }

                  @Override
                  public void onFailure(Throwable t) {}
                },
                executor);
            return delegateFuture;
          }
        },
        MoreExecutors.directExecutor());
  }

  private void storeExists(String name) {
    existsMap.put(name, EMPTY_BYTE_ARRAY);
  }

  private boolean checkExists(String name) {
    return existsMap.containsKey(name) || dataMap.containsKey(name);
  }

  private void storeData(String name, byte[] data) {
    dataMap.put(name, data);
  }

  @Nullable
  private byte[] readData(String name) {
    return dataMap.get(name);
  }

  @Override
  public void close() {
    delegate.close();
    MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.MINUTES);
    mvStore.close();
  }
}

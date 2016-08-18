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

package ws.moor.gletscher.cloud;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class CachingCloudFileStorage implements CloudFileStorage {

  private final CloudFileStorage delegate;
  private final Path localCacheDir;
  private final ListeningExecutorService executor;

  private final Set<String> existingFiles = Collections.synchronizedSet(new TreeSet<>());

  public CachingCloudFileStorage(CloudFileStorage delegate, Path localCacheDir, ListeningExecutorService executor) {
    Preconditions.checkArgument(Files.isDirectory(localCacheDir, LinkOption.NOFOLLOW_LINKS));
    this.delegate = delegate;
    this.localCacheDir = localCacheDir;
    this.executor = executor;
  }

  @Override
  public ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata) {
    ListenableFuture<?> future = delegate.store(name, data, md5, metadata);
    Futures.addCallback(future, new FutureCallback<Object>() {
      @Override
      public void onSuccess(@Nullable Object o) {
        existingFiles.add(name);
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof FileAlreadyExistsException) {
          existingFiles.add(name);
        }
      }
    });
    return future;
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix) {
    return Iterators.transform(delegate.listFiles(prefix), (file) -> {
      existingFiles.add(file.name);
      return file;
    });
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    if (existingFiles.contains(name)) {
      return Futures.immediateFuture(true);
    }

    ListenableFuture<Boolean> future = executor.submit(() -> {
      Path localPath = localCacheDir.resolve(name);
      return Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS);
    });
    future = Futures.transformAsync(future, input -> {
      if (input) {
        return Futures.immediateFuture(true);
      } else {
        return delegate.exists(name);
      }
    });
    Futures.addCallback(future, new FutureCallback<Boolean>() {
      @Override public void onSuccess(@Nullable Boolean exists) {
        if (exists) {
          existingFiles.add(name);
        }
      }
      @Override public void onFailure(Throwable throwable) { }
    });
    return future;
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    Path localPath = localCacheDir.resolve(name);
    ListenableFuture<byte[]> data = executor.submit(() -> {
      if (Files.isRegularFile(localPath, LinkOption.NOFOLLOW_LINKS)) {
        existingFiles.add(name);
        return Files.readAllBytes(localPath);
      } else {
        return null;
      }
    });
    data = Futures.transformAsync(data, bytes -> {
      if (bytes == null) {
        return delegate.get(name);
      } else {
        return Futures.immediateFuture(bytes);
      }
    });
    Futures.addCallback(data, new FutureCallback<byte[]>() {
      @Override public void onSuccess(@Nullable byte[] data) {
        if (data != null) {
          existingFiles.add(name);
          try {
            Files.createDirectories(localPath.getParent());
            Files.write(localPath, data, StandardOpenOption.CREATE_NEW);
          } catch (IOException e) {
            // ignore
          }
        }
      }

      @Override public void onFailure(Throwable t) { }
    }, executor);
    return data;
  }
}

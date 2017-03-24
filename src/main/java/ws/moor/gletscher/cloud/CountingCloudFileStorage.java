/*
 * Copyright 2017 Patrick Moor <patrick@moor.ws>
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

import com.google.common.base.Function;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CountingCloudFileStorage implements CloudFileStorage {

  private final CloudFileStorage delegate;
  private final AtomicInteger storeCount = new AtomicInteger();
  private final AtomicLong storeSize = new AtomicLong();
  private final AtomicInteger existsCount = new AtomicInteger();
  private final AtomicInteger getCount = new AtomicInteger();
  private final AtomicLong getSize = new AtomicLong();

  public CountingCloudFileStorage(CloudFileStorage delegate) {
    this.delegate = delegate;
  }

  @Override
  public ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    storeCount.incrementAndGet();
    storeSize.getAndAdd(data.length);
    return delegate.store(name, data, md5, metadata, options);
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    return delegate.listFiles(prefix, limit);
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    existsCount.incrementAndGet();
    return delegate.exists(name);
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    ListenableFuture<byte[]> future = delegate.get(name);
    return Futures.transform(future, (Function<byte[], byte[]>) input -> {
      getCount.incrementAndGet();
      if (input != null) {
        getSize.addAndGet(input.length);
      }
      return input;
    });
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("store count: ").append(storeCount).append("\n");
    builder.append("store size: ").append(storeSize).append("\n");
    builder.append("exists count: ").append(existsCount).append("\n");
    builder.append("get count: ").append(getCount).append("\n");
    builder.append("get size: ").append(getSize).append("\n");
    return builder.toString();
  }
}

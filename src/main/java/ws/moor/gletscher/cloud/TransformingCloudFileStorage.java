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

import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import ws.moor.gletscher.util.LegacyHashing;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

abstract class TransformingCloudFileStorage implements CloudFileStorage {

  private final CloudFileStorage delegate;
  private final Function<byte[], byte[]> decodingFn =
      new Function<byte[], byte[]>() {
        @Nullable
        @Override
        public byte[] apply(@Nullable byte[] raw) {
          return raw == null ? null : decode(raw);
        }
      };

  TransformingCloudFileStorage(CloudFileStorage delegate) {
    this.delegate = delegate;
  }

  @Override
  public ListenableFuture<?> store(
      String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    byte[] encoded = encode(data);
    HashCode encodedMd5 = LegacyHashing.md5().hashBytes(encoded);

    byte[] restoredData = decode(encoded);
    if (!LegacyHashing.md5().hashBytes(restoredData).equals(md5)) {
      throw new IllegalStateException("oops, return trip does not match");
    }
    if (!Arrays.equals(data, restoredData)) {
      throw new IllegalStateException("oops, even more unlikely!");
    }
    return delegate.store(name, encoded, encodedMd5, metadata, options);
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    return delegate.listFiles(prefix, limit);
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    return delegate.exists(name);
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    return Futures.transform(delegate.get(name), decodingFn::apply, MoreExecutors.directExecutor());
  }

  @Override
  public void close() {
    delegate.close();
  }

  protected abstract byte[] encode(byte[] data);

  protected abstract byte[] decode(byte[] data);
}

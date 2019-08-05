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
import com.google.common.collect.Iterators;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;
import ws.moor.gletscher.blocks.Signature;
import ws.moor.gletscher.util.MoreArrays;
import ws.moor.gletscher.util.Signer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SigningCloudFileStorage implements CloudFileStorage {

  private static final String PROPERTY_NAME = "md5-name-signature";

  private final CloudFileStorage delegate;
  private final Signer signer;

  public SigningCloudFileStorage(CloudFileStorage delegate, Signer signer) {
    this.delegate = delegate;
    this.signer = signer;
  }

  @Override
  public ListenableFuture<?> store(
      String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    Preconditions.checkArgument(!metadata.containsKey(PROPERTY_NAME));

    Map<String, String> attributes = new HashMap<>(metadata);
    attributes.put(PROPERTY_NAME, sign(md5, name).toString());
    return delegate.store(name, data, md5, attributes, options);
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    return Iterators.transform(
        delegate.listFiles(prefix, limit),
        file -> {
          Preconditions.checkArgument(file.metadata.containsKey(PROPERTY_NAME));
          Signature signature = sign(file.md5, file.name);
          if (!signature.toString().equals(file.metadata.get(PROPERTY_NAME))) {
            throw new IllegalStateException("signature mismatch");
          }
          return file;
        });
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    return delegate.exists(name);
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    return delegate.get(name);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private Signature sign(HashCode md5, String name) {
    byte[] dataToSign =
        MoreArrays.concatenate(md5.asBytes(), name.getBytes(StandardCharsets.UTF_8));
    return signer.computeSignature(dataToSign);
  }
}

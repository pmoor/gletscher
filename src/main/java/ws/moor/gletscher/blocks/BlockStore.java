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

package ws.moor.gletscher.blocks;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.util.Signer;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BlockStore {

  private final CloudFileStorage cloudFileStorage;
  private final Signer signer;

  public BlockStore(CloudFileStorage cloudFileStorage, Signer signer) {
    this.cloudFileStorage = cloudFileStorage;
    this.signer = signer;
  }

  public ListenableFuture<PersistedBlock> store(byte[] block, boolean cache) {
    HashCode md5 = Hashing.md5().hashBytes(block);

    int length = block.length;
    Signature signature = signer.computeSignature(block);
    PersistedBlock persisted = new PersistedBlock(signature, length);
    String fileName = toFileName(persisted);

    ListenableFuture<Boolean> existsFuture = cloudFileStorage.exists(fileName);
    return Futures.transformAsync(existsFuture, fileExists -> {
      if (fileExists) {
        return Futures.immediateFuture(persisted);
      } else {
        ListenableFuture<?> future = cloudFileStorage.store(
            fileName, block, md5, ImmutableMap.of(), new CloudFileStorage.StoreOptions(cache));
        ListenableFuture<PersistedBlock> transformed = Futures.transform(future, Functions.constant(persisted));
        return Futures.catching(transformed, CloudFileStorage.FileAlreadyExistsException.class, Functions.constant(persisted));
      }
    });
  }

  public ListenableFuture<byte[]> retrieve(PersistedBlock block) {
    return cloudFileStorage.get(toFileName(block));
  }

  private static String toFileName(PersistedBlock block) {
    Signature signature = block.getSignature();
    return String.format("blocks/%02x/%02x/%s:%d",
        signature.getFirstByte(), signature.getSecondByte(), signature.toString(), block.getOriginalLength());
  }

  public Set<PersistedBlock> listAllBlocks() {
    final Set<PersistedBlock> all = new TreeSet<>();

    ExecutorService executor = Executors.newFixedThreadPool(8);
    for (int i = 0; i < 16; i++) {
      final String prefix = String.format("blocks/%x", i);
      executor.execute(() -> {
        Set<PersistedBlock> set = new TreeSet<>();
        Iterator<CloudFileStorage.FileHeader> it = cloudFileStorage.listFiles(prefix, Integer.MAX_VALUE);
        while (it.hasNext()) {
          CloudFileStorage.FileHeader header = it.next();
          set.add(parseFileName(header.name));
        }
        synchronized (all) {
          all.addAll(set);
        }
      });
    }
    MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.HOURS);
    return all;
  }

  private PersistedBlock parseFileName(String name) {
    Preconditions.checkArgument(name.startsWith("blocks/"));
    int colon = name.lastIndexOf(":");
    int originalLength = Integer.valueOf(name.substring(colon + 1));
    int slash = name.lastIndexOf("/");
    String hex = name.substring(slash + 1, colon);
    Signature signature = Signature.fromString(hex);
    return new PersistedBlock(signature, originalLength);
  }
}

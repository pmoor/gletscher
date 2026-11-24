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

package ws.moor.gletscher.blocks;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.util.LegacyHashing;
import ws.moor.gletscher.util.Signer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BlockStore {

  private final CloudFileStorage cloudFileStorage;
  private final Signer signer;

  public BlockStore(CloudFileStorage cloudFileStorage, Signer signer) {
    this.cloudFileStorage = cloudFileStorage;
    this.signer = signer;
  }

  public ListenableFuture<PersistedBlock> store(
      byte[] block, boolean cache, CloudFileStorage.ObjectType objectType) {
    HashCode md5 = LegacyHashing.md5().hashBytes(block);

    int length = block.length;
    Signature signature = signer.computeSignature(block);
    PersistedBlock persisted = new PersistedBlock(signature, length);
    String fileName = toFileName(persisted);

    ListenableFuture<Boolean> existsFuture = cloudFileStorage.exists(fileName);
    return Futures.transformAsync(
        existsFuture,
        fileExists -> {
          if (fileExists) {
            return Futures.immediateFuture(persisted);
          } else {
            ListenableFuture<?> future =
                cloudFileStorage.store(
                    fileName,
                    block,
                    md5,
                    ImmutableMap.of(),
                    CloudFileStorage.StoreOptions.builder()
                        .setCacheContentsOnUpload(cache)
                        .setObjectType(objectType)
                        .build());
            ListenableFuture<PersistedBlock> transformed =
                Futures.transform(
                    future, Functions.constant(persisted), MoreExecutors.directExecutor());
            return Futures.catching(
                transformed,
                CloudFileStorage.FileAlreadyExistsException.class,
                Functions.constant(persisted),
                MoreExecutors.directExecutor());
          }
        },
        MoreExecutors.directExecutor());
  }

  public ListenableFuture<byte[]> retrieve(PersistedBlock block) {
    return retrieve(block, CloudFileStorage.GetOptions.DEFAULT);
  }

  public ListenableFuture<byte[]> retrieve(
      PersistedBlock block, CloudFileStorage.GetOptions options) {
    return cloudFileStorage.get(toFileName(block), options);
  }

  public static String toFileName(PersistedBlock block) {
    Signature signature = block.getSignature();
    return String.format(
        "blocks/%02x/%02x/%s:%d",
        signature.getFirstByte(),
        signature.getSecondByte(),
        signature.toString(),
        block.getOriginalLength());
  }

  /** Lists all remote block headers under {@code blocks/} in parallel across hex prefixes. */
  public List<CloudFileStorage.FileHeader> listAllBlockHeaders() {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<List<CloudFileStorage.FileHeader>>> futures = new ArrayList<>(16);
    for (int i = 0; i < 16; i++) {
      final String prefix = String.format("blocks/%x", i);
      futures.add(
          executor.submit(
              () -> {
                List<CloudFileStorage.FileHeader> list = new ArrayList<>();
                Iterator<CloudFileStorage.FileHeader> it =
                    cloudFileStorage.listFiles(prefix, Integer.MAX_VALUE);
                while (it.hasNext()) {
                  list.add(it.next());
                }
                return list;
              }));
    }
    List<CloudFileStorage.FileHeader> all = new ArrayList<>();
    try {
      for (Future<List<CloudFileStorage.FileHeader>> future : futures) {
        all.addAll(Futures.getUnchecked(future));
      }
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.HOURS);
    }
    return all;
  }

  public Set<PersistedBlock> listAllBlocks() {
    Set<PersistedBlock> all = new TreeSet<>();
    for (CloudFileStorage.FileHeader header : listAllBlockHeaders()) {
      all.add(parseFileName(header.name));
    }
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

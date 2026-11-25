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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.FluentLogger;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.ByteArrayDataType;
import org.h2.mvstore.type.StringDataType;
import ws.moor.gletscher.proto.Cache;

import javax.annotation.Nullable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CachingCloudFileStorage implements CloudFileStorage {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final String CACHE_FILE_NAME = "cache.mv";
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private static final ImmutableMap<String, Cache.StorageClass> STORAGE_CLASS_TO_PROTO =
      Arrays.stream(Cache.StorageClass.values())
          .filter(c -> c != Cache.StorageClass.UNKNOWN && c != Cache.StorageClass.UNRECOGNIZED)
          .collect(ImmutableMap.toImmutableMap(Enum::name, c -> c));

  private static final MVMap.Builder<String, byte[]> MAP_BUILDER =
      new MVMap.Builder<String, byte[]>()
          .keyType(StringDataType.INSTANCE)
          .valueType(ByteArrayDataType.INSTANCE);

  private final CloudFileStorage delegate;
  private final Path localCacheDir;
  private MVStore mvStore;
  private MVMap<String, byte[]> existsMap;
  private MVMap<String, byte[]> dataMap;
  private MVMap<String, byte[]> listingsMap;
  private final Duration maxListingAge;
  private final Clock clock;
  private final ListeningExecutorService executor;

  public CachingCloudFileStorage(CloudFileStorage delegate, Path localCacheDir) {
    this(delegate, localCacheDir, Duration.ZERO, Clock.systemUTC());
  }

  public CachingCloudFileStorage(
      CloudFileStorage delegate, Path localCacheDir, Duration maxListingAge, Clock clock) {
    Preconditions.checkArgument(Files.isDirectory(localCacheDir, LinkOption.NOFOLLOW_LINKS));
    Preconditions.checkArgument(!maxListingAge.isNegative(), "maxListingAge must be non-negative");
    this.delegate = delegate;
    this.localCacheDir = localCacheDir;
    this.maxListingAge = Preconditions.checkNotNull(maxListingAge);
    this.clock = Preconditions.checkNotNull(clock);
    openMaps();
    this.executor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                1, new ThreadFactoryBuilder().setNameFormat("cache-thread-%d").build()));
  }

  private void openMaps() {
    this.mvStore = openStore(localCacheDir);
    this.existsMap = mvStore.openMap("exists", MAP_BUILDER);
    this.dataMap = mvStore.openMap("data", MAP_BUILDER);
    this.listingsMap = mvStore.openMap("listings", MAP_BUILDER);
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
    if (limit != Integer.MAX_VALUE) {
      return delegate.listFiles(prefix, limit);
    }

    if (!maxListingAge.isZero()) {
      List<FileHeader> cached = tryReadCachedListing(prefix);
      if (cached != null) {
        return cached.iterator();
      }
    }

    Instant retrievedAt = clock.instant();
    Iterator<FileHeader> delegateIterator = delegate.listFiles(prefix, limit);
    List<FileHeader> collected = new ArrayList<>();
    return new AbstractIterator<>() {
      @Override
      protected FileHeader computeNext() {
        if (delegateIterator.hasNext()) {
          FileHeader next = delegateIterator.next();
          collected.add(next);
          return next;
        }
        storeListing(prefix, retrievedAt, collected);
        return endOfData();
      }
    };
  }

  @Nullable
  private List<FileHeader> tryReadCachedListing(String prefix) {
    byte[] raw = listingsMap.get(prefix);
    if (raw == null) {
      return null;
    }
    Cache.CachedListing proto;
    try {
      proto = Cache.CachedListing.parseFrom(raw);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
    Instant retrievedAt = Instant.ofEpochMilli(proto.getRetrievedAtMillis());
    Duration age = Duration.between(retrievedAt, clock.instant());
    if (age.isNegative() || age.compareTo(maxListingAge) > 0) {
      return null;
    }
    List<FileHeader> headers = new ArrayList<>(proto.getFileCount());
    for (Cache.CachedFileHeader entry : proto.getFileList()) {
      headers.add(
          new FileHeader(
              entry.getName(),
              HashCode.fromBytes(entry.getMd5().toByteArray()),
              entry.getSize(),
              ImmutableMap.copyOf(entry.getMetadataMap()),
              decodeStorageClass(entry),
              entry.hasCreatedTimeMillis()
                  ? Instant.ofEpochMilli(entry.getCreatedTimeMillis())
                  : null));
    }
    return headers;
  }

  @Nullable
  private static String decodeStorageClass(Cache.CachedFileHeader entry) {
    if (entry.getStorageClass() != Cache.StorageClass.UNKNOWN
        && entry.getStorageClass() != Cache.StorageClass.UNRECOGNIZED) {
      return entry.getStorageClass().name();
    }
    return null;
  }

  private void storeListing(String prefix, Instant retrievedAt, List<FileHeader> headers) {
    Cache.CachedListing.Builder listingBuilder =
        Cache.CachedListing.newBuilder().setRetrievedAtMillis(retrievedAt.toEpochMilli());
    for (FileHeader header : headers) {
      Cache.CachedFileHeader.Builder entryBuilder =
          Cache.CachedFileHeader.newBuilder()
              .setName(header.name)
              .setMd5(ByteString.copyFrom(header.md5.asBytes()))
              .setSize(header.size)
              .putAllMetadata(header.metadata);
      if (header.storageClass != null) {
        Cache.StorageClass protoClass =
            STORAGE_CLASS_TO_PROTO.get(header.storageClass.toUpperCase(Locale.ROOT));
        if (protoClass != null) {
          entryBuilder.setStorageClass(protoClass);
        } else {
          logger
              .atSevere()
              .atMostEvery(10, TimeUnit.SECONDS)
              .log("Unknown storage class '%s' for %s; storing as UNKNOWN",
                  header.storageClass, header.name);
          entryBuilder.setStorageClass(Cache.StorageClass.UNKNOWN);
        }
      }
      if (header.createdTime != null) {
        entryBuilder.setCreatedTimeMillis(header.createdTime.toEpochMilli());
      }
      listingBuilder.addFile(entryBuilder.build());
    }
    listingsMap.put(prefix, listingBuilder.build().toByteArray());
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
  public ListenableFuture<byte[]> get(String name, GetOptions options) {
    if (!options.readFromCache) {
      return fetchFromDelegate(name, options);
    }
    ListenableFuture<byte[]> data = executor.submit(() -> readData(name));
    return Futures.transformAsync(
        data,
        bytes -> {
          if (bytes != null) {
            return Futures.immediateFuture(bytes);
          } else {
            return fetchFromDelegate(name, options);
          }
        },
        MoreExecutors.directExecutor());
  }

  private ListenableFuture<byte[]> fetchFromDelegate(String name, GetOptions options) {
    ListenableFuture<byte[]> delegateFuture = delegate.get(name, options);
    if (options.populateCache) {
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
    }
    return delegateFuture;
  }

  /** Statistics returned by {@link #purgeCache(Set, boolean)}. */
  public record PurgeStats(
      int removedDataCount,
      long removedDataBytes,
      int keptDataCount,
      long keptDataBytes,
      int removedExistsCount,
      int keptExistsCount,
      int removedListingsCount) {}

  /**
   * Removes cached object payloads whose keys are not in {@code keysToKeep}, clears cached prefix
   * listings, optionally purges cached exists markers not in {@code keysToKeep}, and compacts the
   * store.
   */
  public PurgeStats purgeCache(Set<String> keysToKeep, boolean purgeExists) {
    // Wait for any pending async cache writes to finish before mutating or compacting the store.
    Futures.getUnchecked(executor.submit(() -> {}));

    List<String> dataKeysToRemove = new ArrayList<>();
    int keptDataCount = 0;
    long keptDataBytes = 0;
    for (Map.Entry<String, byte[]> entry : dataMap.entrySet()) {
      if (keysToKeep.contains(entry.getKey())) {
        keptDataCount++;
        keptDataBytes += entry.getValue().length;
      } else {
        dataKeysToRemove.add(entry.getKey());
      }
    }

    long removedDataBytes = 0;
    for (String key : dataKeysToRemove) {
      byte[] removed = dataMap.remove(key);
      if (removed != null) {
        removedDataBytes += removed.length;
      }
    }

    int removedExistsCount = 0;
    int keptExistsCount = 0;
    if (purgeExists) {
      List<String> existsKeysToRemove = new ArrayList<>();
      for (String key : existsMap.keySet()) {
        if (keysToKeep.contains(key)) {
          keptExistsCount++;
        } else {
          existsKeysToRemove.add(key);
        }
      }
      for (String key : existsKeysToRemove) {
        if (existsMap.remove(key) != null) {
          removedExistsCount++;
        }
      }
    }

    int removedListingsCount = listingsMap.size();
    if (removedListingsCount > 0) {
      listingsMap.clear();
    }

    if (!dataKeysToRemove.isEmpty() || removedExistsCount > 0 || removedListingsCount > 0) {
      mvStore.commit();
      if (localCacheDir.getFileSystem().equals(FileSystems.getDefault())) {
        mvStore.close();
        MVStore.compact(localCacheDir.resolve(CACHE_FILE_NAME).toString(), false, null);
        openMaps();
      }
    }
    return new PurgeStats(
        dataKeysToRemove.size(),
        removedDataBytes,
        keptDataCount,
        keptDataBytes,
        removedExistsCount,
        keptExistsCount,
        removedListingsCount);
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

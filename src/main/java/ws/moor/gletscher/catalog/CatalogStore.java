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

package ws.moor.gletscher.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.LegacyHashing;

import java.nio.file.FileSystem;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public class CatalogStore {

  private static final String VERSION_META_KEY = "version";

  private final FileSystem fs;
  private final CloudFileStorage storage;
  private final BlockStore blockStore;
  private final DateTimeFormatter dateTimeFormatter;

  public CatalogStore(FileSystem fs, CloudFileStorage storage, BlockStore blockStore) {
    this.fs = fs;
    this.storage = storage;
    this.blockStore = blockStore;
    dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss", Locale.US).withZone(ZoneId.of("UTC"));
  }

  public PersistedBlock store(Catalog catalog) {
    String fileName = createFileName(catalog.getStartTime());
    byte[] data = catalog.toProto().toByteArray();

    PersistedBlock pb =
        Futures.getUnchecked(
            blockStore.store(data, true, CloudFileStorage.ObjectType.METADATA));
    byte[] bytes = pb.toProto().toByteArray();
    Futures.getUnchecked(storage.store(
        fileName,
        bytes,
        LegacyHashing.md5().hashBytes(bytes),
        ImmutableMap.of(VERSION_META_KEY, "1"),
        CloudFileStorage.StoreOptions.builder()
            .setCacheContentsOnUpload(true)
            .setObjectType(CloudFileStorage.ObjectType.METADATA)
            .build()));
    return pb;
  }

  private String createFileName(Instant now) {
    long orderKey = (Long.MAX_VALUE - now.toEpochMilli());
    return String.format("backups/%016x-%s", orderKey, dateTimeFormatter.format(now));
  }

  public Catalog load(PersistedBlock address) {
    try {
      byte[] bytes = Futures.getUnchecked(blockStore.retrieve(address));
      Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(bytes);
      return Catalog.fromProto(address, fs, proto);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A stored backup snapshot pointer together with its catalog block (for v1) and parsed catalog. */
  public record Snapshot(
      String pointerName, Optional<PersistedBlock> catalogBlock, Catalog catalog) {}

  public Optional<Catalog> getLatestCatalog() {
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", 1);
    if (!it.hasNext()) {
      return Optional.empty();
    }
    return Optional.of(loadSnapshot(it.next(), true).catalog());
  }

  /** Lists up to {@code limit} snapshot pointer headers from newest to oldest. */
  public List<CloudFileStorage.FileHeader> listSnapshotHeaders(int limit) {
    List<CloudFileStorage.FileHeader> headers = new ArrayList<>();
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", limit);
    while (it.hasNext()) {
      headers.add(it.next());
    }
    return headers;
  }

  /** Loads and parses a single snapshot pointer without modifying remote storage. */
  public Snapshot loadSnapshot(CloudFileStorage.FileHeader header) {
    return loadSnapshot(header, false);
  }

  /**
   * Lists up to {@code limit} snapshots from newest to oldest without modifying remote storage.
   */
  public List<Snapshot> listSnapshots(int limit) {
    Preconditions.checkArgument(limit >= 0, "limit must be non-negative: %s", limit);
    if (limit == 0) {
      return List.of();
    }
    List<CloudFileStorage.FileHeader> headers = listSnapshotHeaders(limit);
    List<Snapshot> snapshots = new ArrayList<>(headers.size());
    for (CloudFileStorage.FileHeader header : headers) {
      snapshots.add(loadSnapshot(header));
    }
    return snapshots;
  }

  /**
   * Collects all remote metadata object names belonging to {@code snapshots}: backup pointers,
   * catalog blocks, and all reachable directory blocks (deduplicated across snapshots).
   */
  public Set<String> collectMetadataObjectNames(
      List<Snapshot> snapshots, boolean followBaseCatalogs) {
    return collectReachableObjectNames(snapshots, false, followBaseCatalogs);
  }

  /**
   * Collects all remote object names reachable from {@code snapshots}: backup pointers, catalog
   * blocks, directory blocks, and referenced file data blocks (deduplicated across snapshots).
   */
  public Set<String> collectAllReachableObjectNames(
      List<Snapshot> snapshots, boolean followBaseCatalogs) {
    return collectReachableObjectNames(snapshots, true, followBaseCatalogs);
  }

  private Set<String> collectReachableObjectNames(
      List<Snapshot> snapshots, boolean includeDataBlocks, boolean followBaseCatalogs) {
    Set<String> names = new LinkedHashSet<>();
    Set<PersistedBlock> visitedCatalogs = new HashSet<>();
    Set<PersistedBlock> visitedDirectories = new HashSet<>();
    Deque<Catalog> catalogQueue = new ArrayDeque<>();
    Deque<PersistedBlock> dirQueue = new ArrayDeque<>();

    for (Snapshot snapshot : snapshots) {
      names.add(snapshot.pointerName());
      snapshot
          .catalogBlock()
          .ifPresent(
              pb -> {
                names.add(BlockStore.toFileName(pb));
                visitedCatalogs.add(pb);
              });
      catalogQueue.push(snapshot.catalog());
    }

    while (!catalogQueue.isEmpty()) {
      Catalog catalog = catalogQueue.pop();
      for (PersistedBlock rootBlock : catalog.getRoots().values()) {
        if (visitedDirectories.add(rootBlock)) {
          dirQueue.push(rootBlock);
        }
      }
      if (followBaseCatalogs) {
        catalog
            .getBaseCatalog()
            .ifPresent(
                baseBlock -> {
                  String fileName = BlockStore.toFileName(baseBlock);
                  if (visitedCatalogs.add(baseBlock)
                      && Boolean.TRUE.equals(Futures.getUnchecked(storage.exists(fileName)))) {
                    names.add(fileName);
                    catalogQueue.push(load(baseBlock));
                  }
                });
      }
    }

    while (!dirQueue.isEmpty()) {
      PersistedBlock dirBlock = dirQueue.pop();
      names.add(BlockStore.toFileName(dirBlock));
      byte[] bytes = Futures.getUnchecked(blockStore.retrieve(dirBlock));
      try {
        Gletscher.Directory dir = Gletscher.Directory.parseFrom(bytes);
        for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
          if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
            PersistedBlock childBlock = PersistedBlock.fromProto(entry.getDirectory().getBlock());
            if (visitedDirectories.add(childBlock)) {
              dirQueue.push(childBlock);
            }
          } else if (includeDataBlocks
              && entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
            for (Gletscher.PersistedBlock fileBlock : entry.getFile().getBlockList()) {
              names.add(BlockStore.toFileName(PersistedBlock.fromProto(fileBlock)));
            }
          }
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Failed to parse directory block: " + dirBlock, e);
      }
    }

    return names;
  }

  private static boolean isVersion1(CloudFileStorage.FileHeader header) {
    return header.metadata.containsKey(VERSION_META_KEY)
        && Integer.parseInt(header.metadata.get(VERSION_META_KEY)) == 1;
  }

  private Snapshot loadSnapshot(CloudFileStorage.FileHeader header, boolean persistLegacyCatalog) {
    try {
      byte[] catalogFileBytes = Futures.getUnchecked(storage.get(header.name));
      if (isVersion1(header)) {
        Gletscher.PersistedBlock pbProto = Gletscher.PersistedBlock.parseFrom(catalogFileBytes);
        PersistedBlock pb = PersistedBlock.fromProto(pbProto);
        return new Snapshot(header.name, Optional.of(pb), load(pb));
      } else {
        Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(catalogFileBytes);
        PersistedBlock pb = null;
        if (persistLegacyCatalog) {
          // Store this old-style catalog as a block as well, and use its address.
          pb =
              Futures.getUnchecked(
                  blockStore.store(catalogFileBytes, true, CloudFileStorage.ObjectType.METADATA));
        }
        return new Snapshot(header.name, Optional.ofNullable(pb), Catalog.fromProto(pb, fs, proto));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}

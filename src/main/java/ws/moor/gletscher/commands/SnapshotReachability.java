/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.commands;

import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Shared helpers for collecting metadata and data object names reachable from snapshots. */
final class SnapshotReachability {

  private record DirVisit(PersistedBlock block, boolean optionalParentLink) {}

  static List<CatalogStore.Snapshot> listSnapshots(CatalogStore catalogStore, int limit) {
    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(limit);
    List<CatalogStore.Snapshot> snapshots = new ArrayList<>(headers.size());
    for (CloudFileStorage.FileHeader header : headers) {
      snapshots.add(catalogStore.loadSnapshot(header));
    }
    return snapshots;
  }

  static Set<String> collectReachableObjectNames(
      CloudFileStorage cloudFileStorage,
      BlockStore blockStore,
      CatalogStore catalogStore,
      List<CatalogStore.Snapshot> snapshots,
      boolean includeDataBlocks,
      boolean followParentLinks) {
    Set<String> names = new LinkedHashSet<>();
    Set<PersistedBlock> visitedCatalogs = new HashSet<>();
    Set<PersistedBlock> visitedDirectories = new HashSet<>();
    Deque<Catalog> catalogQueue = new ArrayDeque<>();
    Deque<DirVisit> dirQueue = new ArrayDeque<>();

    for (CatalogStore.Snapshot snapshot : snapshots) {
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
          dirQueue.addFirst(new DirVisit(rootBlock, false));
        }
      }
      if (followParentLinks) {
        catalog
            .getBaseCatalog()
            .ifPresent(
                baseBlock -> {
                  String fileName = BlockStore.toFileName(baseBlock);
                  if (visitedCatalogs.add(baseBlock)
                      && Boolean.TRUE.equals(
                          Futures.getUnchecked(cloudFileStorage.exists(fileName)))) {
                    names.add(fileName);
                    catalogQueue.push(catalogStore.load(baseBlock));
                  }
                });
      }
    }

    while (!dirQueue.isEmpty()) {
      DirVisit visit = dirQueue.removeFirst();
      PersistedBlock dirBlock = visit.block();
      byte[] bytes = Futures.getUnchecked(blockStore.retrieve(dirBlock));
      if (bytes == null) {
        if (visit.optionalParentLink()) {
          continue;
        }
        throw new IllegalStateException("Missing directory block: " + dirBlock);
      }
      names.add(BlockStore.toFileName(dirBlock));
      try {
        Gletscher.Directory dir = Gletscher.Directory.parseFrom(bytes);
        if (followParentLinks && dir.hasPreviousVersion()) {
          PersistedBlock previousBlock = PersistedBlock.fromProto(dir.getPreviousVersion());
          if (visitedDirectories.add(previousBlock)) {
            dirQueue.addLast(new DirVisit(previousBlock, true));
          }
        }
        for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
          if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
            PersistedBlock childBlock = PersistedBlock.fromProto(entry.getDirectory().getBlock());
            if (visitedDirectories.add(childBlock)) {
              dirQueue.addFirst(new DirVisit(childBlock, false));
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

  private SnapshotReachability() {}
}

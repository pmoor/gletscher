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

package ws.moor.gletscher.catalog;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class CatalogReader {

  public static final class FileInformation {

    public final Instant lastModifiedTime;
    public final List<PersistedBlock> blockList;

    FileInformation(Instant lastModifiedTime, List<PersistedBlock> blockList) {
      this.lastModifiedTime = lastModifiedTime;
      this.blockList = blockList;
    }
  }

  private static final Gletscher.Directory NULL_DIR = Gletscher.Directory.getDefaultInstance();

  private final BlockStore blockStore;
  private final PersistedBlock root;
  private final LoadingCache<Path, Gletscher.Directory> dirCache = CacheBuilder.newBuilder()
      .maximumWeight(4 << 20) // 4 MB
      .weigher((Weigher<Path, Gletscher.Directory>) (k, v) -> k.toString().length() + v.getSerializedSize())
      .build(CacheLoader.from(this::findDirectory));

  public CatalogReader(BlockStore blockStore, PersistedBlock root) {
    this.blockStore = blockStore;
    this.root = root;
  }

  public FileInformation findFile(Path path) {
    Gletscher.Directory parentDirectory = dirCache.getUnchecked(path.getParent());
    if (parentDirectory == NULL_DIR) {
      return null;
    }

    for (Gletscher.DirectoryEntry entry : parentDirectory.getEntryList()) {
      if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
        Gletscher.FileEntry fileProto = entry.getFile();
        if (fileProto.getName().equals(path.getFileName().toString())) {
          ImmutableList.Builder<PersistedBlock> builder = ImmutableList.builder();
          for (Gletscher.PersistedBlock persistedBlock : fileProto.getBlockList()) {
            builder.add(PersistedBlock.fromProto(persistedBlock));
          }
          return new FileInformation(Instant.ofEpochMilli(fileProto.getLastModifiedMillis()), builder.build());
        }
      }
    }
    return null;
  }

  private Gletscher.Directory findDirectory(Path dir) {
    if (dir.getParent() == null) {
      return fetchDir(this.root);
    }

    Gletscher.Directory parent = dirCache.getUnchecked(dir.getParent());
    for (Gletscher.DirectoryEntry entry : parent.getEntryList()) {
      if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
        Gletscher.ChildDirectoryEntry dirProto = entry.getDirectory();
        if (dirProto.getName().equals(dir.getFileName().toString())) {
          return fetchDir(PersistedBlock.fromProto(dirProto.getBlock()));
        }
      }
    }
    return NULL_DIR;
  }

  private Gletscher.Directory fetchDir(PersistedBlock block){
    try {
      byte[] data = Futures.getUnchecked(blockStore.retrieve(block));
      if (data == null) {
        return NULL_DIR;
      }
      return Gletscher.Directory.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}

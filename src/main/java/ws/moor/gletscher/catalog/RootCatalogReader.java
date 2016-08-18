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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;

public class RootCatalogReader implements CatalogReader {

  private static final Gletscher.Directory NULL_DIR = Gletscher.Directory.getDefaultInstance();

  private final BlockStore blockStore;
  private final Catalog catalog;
  private final LoadingCache<Path, Gletscher.Directory> dirCache = CacheBuilder.newBuilder()
      .maximumWeight(4 << 20) // 4 MB
      .weigher((Weigher<Path, Gletscher.Directory>) (k, v) -> k.toString().length() + v.getSerializedSize())
      .build(CacheLoader.from(this::findDirectory));

  public RootCatalogReader(BlockStore blockStore, Catalog catalog) {
    this.blockStore = blockStore;
    this.catalog = catalog;
  }

  @Override
  public FileInformation findFile(Path path) {
    Gletscher.Directory parentDirectory = dirCache.getUnchecked(path.getParent());
    if (parentDirectory == NULL_DIR) {
      return null;
    }

    for (Gletscher.DirectoryEntry entry : parentDirectory.getEntryList()) {
      if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
        Gletscher.FileEntry fileProto = entry.getFile();
        if (fileProto.getName().equals(path.getFileName().toString())) {
          return toFileInfo(path, fileProto);
        }
      }
    }
    return null;
  }

  private FileInformation toFileInfo(Path name, Gletscher.FileEntry fileProto) {
    ImmutableList.Builder<PersistedBlock> builder = ImmutableList.builder();
    for (Gletscher.PersistedBlock persistedBlock : fileProto.getBlockList()) {
      builder.add(PersistedBlock.fromProto(persistedBlock));
    }
    return new FileInformation(name, Instant.ofEpochMilli(fileProto.getLastModifiedMillis()), builder.build());
  }

  public Iterator<FileInformation> walk() {
    final Deque<PathAndBlock> stack = new ArrayDeque<>();
    for (Map.Entry<Path, PersistedBlock> entry : catalog.getRoots().entrySet()) {
      stack.add(new PathAndBlock(entry.getKey(), entry.getValue()));
    }
    return new AbstractIterator<FileInformation>() {
      private Path currentPath;
      private Iterator<Gletscher.DirectoryEntry> currentDir;

      @Override
      protected FileInformation computeNext() {
        while (currentDir != null && currentDir.hasNext()) {
          Gletscher.DirectoryEntry next = currentDir.next();
          switch (next.getTypeCase()) {
            case FILE:
              return toFileInfo(currentPath.resolve(next.getFile().getName()), next.getFile());
            case DIRECTORY:
              stack.push(new PathAndBlock(
                  currentPath.resolve(next.getDirectory().getName()),
                  PersistedBlock.fromProto(next.getDirectory().getBlock())));
              break;
            case SYMLINK:
              // TODO(pmoor): implement
              break;
            default:
              throw new IllegalArgumentException(next.getTypeCase().toString());
          }
        }

        if (stack.isEmpty()) {
          return endOfData();
        }

        PathAndBlock nextPair = stack.pop();
        Gletscher.Directory nextDir = fetchDir(nextPair.block);
        currentPath = nextPair.path;
        currentDir = nextDir.getEntryList().iterator();
        return computeNext();
      }
    };
  }

  private static class PathAndBlock {
    final Path path;
    final PersistedBlock block;

    private PathAndBlock(Path path, PersistedBlock block) {
      this.path = path;
      this.block = block;
    }
  }

  private Gletscher.Directory findDirectory(Path dir) {
    if (dir.getParent() == null) {
      Preconditions.checkState(dir.getRoot().equals(dir));
      return fetchDir(catalog.getRootBlock(dir.getRoot()));
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
    byte[] data = Futures.getUnchecked(blockStore.retrieve(block));
    if (data == null) {
      return NULL_DIR;
    }
    return parseDirectory(data);
  }

  private Gletscher.Directory parseDirectory(byte[] data) {
    try {
      return Gletscher.Directory.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}

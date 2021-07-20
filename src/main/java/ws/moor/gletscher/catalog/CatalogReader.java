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

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CatalogReader {

  private static final Gletscher.Directory NULL_DIR = Gletscher.Directory.getDefaultInstance();

  private final BlockStore blockStore;
  private final Catalog catalog;
  private final LoadingCache<Path, CacheEntry> dirCache =
      CacheBuilder.newBuilder()
          .maximumWeight(4 << 20) // 4 MB
          .weigher((Weigher<Path, CacheEntry>) (k, v) -> k.toString().length() + v.weight())
          .build(CacheLoader.from(this::loadDirectory));

  public CatalogReader(BlockStore blockStore, Catalog catalog) {
    this.blockStore = blockStore;
    this.catalog = catalog;
  }

  public DirectoryInformation findDirectory(Path path) {
    CacheEntry cacheEntry = dirCache.getUnchecked(path);
    if (cacheEntry.directory == NULL_DIR) {
      return null;
    }

    return new DirectoryInformation(path, cacheEntry);
  }

  public static final class FileInformation {

    public final Path path;
    public final Instant lastModifiedTime;
    public final List<PersistedBlock> blockList;

    FileInformation(Path path, Instant lastModifiedTime, List<PersistedBlock> blockList) {
      this.path = path;
      this.lastModifiedTime = lastModifiedTime;
      this.blockList = blockList;
    }

    public long getOriginalSize() {
      return blockList.stream().collect(Collectors.summingLong(PersistedBlock::getOriginalLength));
    }
  }

  public static final class DirectoryInformation {

    private final CacheEntry cacheEntry;
    private final Map<String, FileInformation> fileEntries = new TreeMap<>();

    DirectoryInformation(Path path, CacheEntry cacheEntry) {
      this.cacheEntry = cacheEntry;
      for (Gletscher.DirectoryEntry entry : cacheEntry.directory.getEntryList()) {
        if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
          Gletscher.FileEntry fileProto = entry.getFile();
          String fileName = fileProto.getName();
          try {
            Path filePath = path.resolve(fileName);
            fileEntries.put(fileName, toFileInfo(filePath, fileProto));
          } catch (InvalidPathException ipe) {
            System.err.printf("failed to resolve \"%s\" from \"%s\": %s", fileName, path, ipe);
          }
        }
      }
    }

    public FileInformation findFileInformation(String fileName) {
      return fileEntries.get(fileName);
    }

    public PersistedBlock getAddress() {
      return cacheEntry.address;
    }

    public boolean hasChanged(Gletscher.Directory newProto) {
      Gletscher.Directory oldProto = cacheEntry.directory;
      if (oldProto.getEntryCount() != newProto.getEntryCount()) {
        return true;
      }
      for (int i = 0; i < newProto.getEntryCount(); i++) {
        Gletscher.DirectoryEntry oldEntry = oldProto.getEntry(i);
        Gletscher.DirectoryEntry newEntry = newProto.getEntry(i);
        if (!oldEntry.equals(newEntry)) {
          return true;
        }
      }
      return false;
    }
  }

  private static FileInformation toFileInfo(Path name, Gletscher.FileEntry fileProto) {
    ImmutableList.Builder<PersistedBlock> builder = ImmutableList.builder();
    for (Gletscher.PersistedBlock persistedBlock : fileProto.getBlockList()) {
      builder.add(PersistedBlock.fromProto(persistedBlock));
    }
    return new FileInformation(
        name, Instant.ofEpochMilli(fileProto.getLastModifiedMillis()), builder.build());
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
              stack.push(
                  new PathAndBlock(
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

  private CacheEntry loadDirectory(Path dir) {
    if (dir.getParent() == null) {
      Preconditions.checkState(dir.getRoot().equals(dir));
      PersistedBlock rootBlock = catalog.getRootBlock(dir.getRoot());
      if (rootBlock == null) {
        return new CacheEntry(null, NULL_DIR);
      }
      return new CacheEntry(rootBlock, fetchDir(rootBlock));
    }

    CacheEntry parent = dirCache.getUnchecked(dir.getParent());
    for (Gletscher.DirectoryEntry entry : parent.directory.getEntryList()) {
      if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
        Gletscher.ChildDirectoryEntry dirProto = entry.getDirectory();
        if (dirProto.getName().equals(dir.getFileName().toString())) {
          PersistedBlock block = PersistedBlock.fromProto(dirProto.getBlock());
          return new CacheEntry(block, fetchDir(block));
        }
      }
    }
    return new CacheEntry(null, NULL_DIR);
  }

  private Gletscher.Directory fetchDir(PersistedBlock block) {
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

  private static class CacheEntry {
    private final PersistedBlock address;
    private final Gletscher.Directory directory;

    CacheEntry(PersistedBlock address, Gletscher.Directory directory) {
      this.address = address;
      this.directory = directory;
    }

    public int weight() {
      return directory.getSerializedSize() + 32;
    }
  }
}

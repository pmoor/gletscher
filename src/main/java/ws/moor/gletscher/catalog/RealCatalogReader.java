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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;

class RealCatalogReader implements CatalogReader {
  private static final Gletscher.Directory NULL_DIR = Gletscher.Directory.getDefaultInstance();

  private final BlockStore blockStore;
  private final Catalog catalog;
  private final LoadingCache<CatalogPath, CacheEntry> dirCache =
      CacheBuilder.newBuilder()
          .maximumWeight(4 << 20) // 4 MB
          .weigher((Weigher<CatalogPath, CacheEntry>) (k, v) -> k.approximateByteUsage() + v.weight())
          .build(CacheLoader.from(this::loadDirectory));

  RealCatalogReader(BlockStore blockStore, Catalog catalog) {
    this.blockStore = blockStore;
    this.catalog = catalog;
  }

  @Override
  public CatalogDirectory findDirectory(CatalogPath dirPath) {
    CacheEntry cacheEntry = dirCache.getUnchecked(dirPath);
    if (cacheEntry.directory == NULL_DIR) {
      return null;
    }


    return new CatalogDirectory(dirPath, cacheEntry.address, cacheEntry.directory);
  }

  @Override
  public Iterator<CatalogFile> walk() {
    return new CatalogWalker(new Visitor() {});
  }

  @Override
  public void walk(Visitor visitor) {
    CatalogWalker walker = new CatalogWalker(visitor);
    while (walker.hasNext()) {
      walker.next();
    }
  }

  private final class CatalogWalker extends AbstractIterator<CatalogFile> {
    private final Visitor visitor;
    private final Iterator<Map.Entry<CatalogPath, PersistedBlock>> roots =
        catalog.getRoots().entrySet().iterator();
    private final Deque<WalkFrame> stack = new ArrayDeque<>();
    private boolean terminated = false;

    private CatalogWalker(Visitor visitor) {
      this.visitor = visitor;
    }

    @Override
    protected CatalogFile computeNext() {
      while (!terminated) {
        if (stack.isEmpty()) {
          if (!roots.hasNext()) {
            return endOfData();
          }
          Map.Entry<CatalogPath, PersistedBlock> root = roots.next();
          VisitResult rootResult = visitor.preVisitDirectory(root.getKey(), root.getValue());
          if (rootResult == VisitResult.TERMINATE) {
            terminated = true;
            return endOfData();
          }
          if (rootResult == VisitResult.CONTINUE) {
            stack.push(
                new WalkFrame(
                    root.getKey(),
                    root.getValue(),
                    fetchDir(root.getValue()).getEntryList().iterator()));
          }
          continue;
        }

        WalkFrame frame = stack.peek();
        if (!frame.entries.hasNext()) {
          stack.pop();
          if (visitor.postVisitDirectory(frame.path, frame.block) == VisitResult.TERMINATE) {
            terminated = true;
            return endOfData();
          }
          continue;
        }

        Gletscher.DirectoryEntry next = frame.entries.next();
        switch (next.getTypeCase()) {
          case FILE -> {
            CatalogFile file =
                new CatalogFile(frame.path.makeChild(next.getFile().getName()), next.getFile());
            if (visitor.visitFile(file) == VisitResult.TERMINATE) {
              terminated = true;
              return endOfData();
            }
            return file;
          }
          case DIRECTORY -> {
            CatalogPath childPath = frame.path.makeChild(next.getDirectory().getName());
            PersistedBlock childBlock = PersistedBlock.fromProto(next.getDirectory().getBlock());
            VisitResult dirResult = visitor.preVisitDirectory(childPath, childBlock);
            if (dirResult == VisitResult.TERMINATE) {
              terminated = true;
              return endOfData();
            }
            if (dirResult == VisitResult.CONTINUE) {
              stack.push(
                  new WalkFrame(
                      childPath, childBlock, fetchDir(childBlock).getEntryList().iterator()));
            }
          }
          case SYMLINK -> {
            // TODO(pmoor): implement
          }
          default -> throw new IllegalArgumentException(next.getTypeCase().toString());
        }
      }
      return endOfData();
    }
  }

  private static class WalkFrame {
    final CatalogPath path;
    final PersistedBlock block;
    final Iterator<Gletscher.DirectoryEntry> entries;

    private WalkFrame(
        CatalogPath path, PersistedBlock block, Iterator<Gletscher.DirectoryEntry> entries) {
      this.path = path;
      this.block = block;
      this.entries = entries;
    }
  }

  private CacheEntry loadDirectory(CatalogPath dirPath) {
    if (dirPath.isRoot()) {
      PersistedBlock rootBlock = catalog.getRootBlock(dirPath);
      if (rootBlock == null) {
        return new CacheEntry(null, NULL_DIR);
      }
      return new CacheEntry(rootBlock, fetchDir(rootBlock));
    }

    CacheEntry parent = dirCache.getUnchecked(dirPath.getParent());
    for (Gletscher.DirectoryEntry entry : parent.directory.getEntryList()) {
      if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
        Gletscher.ChildDirectoryEntry dirProto = entry.getDirectory();
        if (dirProto.getName().equals(dirPath.getFileName())) {
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

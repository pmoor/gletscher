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

import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.ByteSize;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class CatalogAnalyzer {

  private final BlockStore blockStore;

  public CatalogAnalyzer(BlockStore blockStore) {
    this.blockStore = blockStore;
  }

  public void analyze(Catalog catalog, PrintStream out) {
    out.printf("Analyzing catalog %s...\n", catalog.getAddress());

    Stack<Gletscher.Directory> stack = new Stack<>();
    for (PersistedBlock root : catalog.getRoots().values()) {
      stack.push(fetchDirectory(root));
    }

    int directories = 0;
    int files = 0;
    int symlinks = 0;
    int totalBlocks = 0;
    long totalBlockSize = 0;
    long metaSize = 0;
    Set<PersistedBlock> uniqueBlocks = new HashSet<>();
    while (!stack.isEmpty()) {
      directories++;
      Gletscher.Directory dir = stack.pop();
      metaSize += dir.getSerializedSize();
      for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
        switch (entry.getTypeCase()) {
          case FILE:
            files++;
            for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
              uniqueBlocks.add(PersistedBlock.fromProto(block));
              totalBlocks++;
              totalBlockSize += block.getOriginalSize();
            }
            break;
          case DIRECTORY:
            stack.push(fetchDirectory(PersistedBlock.fromProto(entry.getDirectory().getBlock())));
            break;
          case SYMLINK:
            symlinks++;
            break;
          default:
            throw new IllegalStateException(entry.toString());
        }
      }
    }

    long uniqueSize =
        uniqueBlocks.stream().collect(Collectors.summingLong(PersistedBlock::getOriginalLength));

    out.println("            roots: " + catalog.getRoots().size());
    out.println("      directories: " + directories);
    out.println("            files: " + files);
    out.println("         symlinks: " + symlinks);
    out.println("        meta size: " + ByteSize.ofBytes(metaSize));
    out.println("     total blocks: " + totalBlocks);
    out.println(" total block size: " + ByteSize.ofBytes(totalBlockSize));
    out.println("    unique blocks: " + uniqueBlocks.size());
    out.println("unique block size: " + ByteSize.ofBytes(uniqueSize));
    out.println();

    //    Set<PersistedBlock> allBlocksThere = blockStore.listAllBlocks();
    //    System.out.println("all blocks in storage: " + allBlocksThere.size());
    //    long totalSize = 0;
    //    for (PersistedBlock block : allBlocksThere) {
    //      totalSize += block.getOriginalLength();
    //    }
    //    System.out.println("all blocks in storage size: " + totalSize);
    //
    //    allBlocksThere.removeAll(uniqueBlocks);
    //    System.out.println("excess blocks: " + allBlocksThere.size());
    //    totalSize = 0;
    //    for (PersistedBlock block : allBlocksThere) {
    //      totalSize += block.getOriginalLength();
    //    }
    //    System.out.println("excess blocks size: " + totalSize);
  }

  private Gletscher.Directory fetchDirectory(PersistedBlock root) {
    try {
      return Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(root)));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}

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
import ws.moor.gletscher.util.Histogram;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CatalogAnalyzer {

  private final BlockStore blockStore;
  private final boolean printHistograms;

  public CatalogAnalyzer(BlockStore blockStore, boolean printHistograms) {
    this.blockStore = blockStore;
    this.printHistograms = printHistograms;
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
    Histogram blockSizes = new Histogram(0, 100, 1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20);
    Histogram metaSizes = new Histogram(0, 100, 1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20, 3 << 20, 4 << 20);
    Histogram fileSizes = new Histogram(0, 1 << 10, 1 << 20, 1 << 30, 1L << 40);
    Set<PersistedBlock> uniqueBlocks = new HashSet<>();
    Map<String, AtomicLong> bytesByExtension = new HashMap<>();
    while (!stack.isEmpty()) {
      directories++;
      Gletscher.Directory dir = stack.pop();
      metaSizes.add(dir.getSerializedSize());
      for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
        switch (entry.getTypeCase()) {
          case FILE:
            files++;
            long fileSize = 0;
            for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
              uniqueBlocks.add(PersistedBlock.fromProto(block));
              blockSizes.add(block.getOriginalSize());
              fileSize += block.getOriginalSize();
            }
            fileSizes.add(fileSize);
            trackBytesByExtension(entry.getFile().getName(), fileSize, bytesByExtension);
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
    out.println("        meta size: " + ByteSize.ofBytes(metaSizes.sum()));
    out.println("     total blocks: " + blockSizes.count());
    out.println(" total block size: " + ByteSize.ofBytes(blockSizes.sum()));
    out.println("    unique blocks: " + uniqueBlocks.size());
    out.println("unique block size: " + ByteSize.ofBytes(uniqueSize));
    out.println();

    if (printHistograms) {
      PrintWriter writer = new PrintWriter(out);
      writer.println("File Blocks:");
      blockSizes.write(it -> ByteSize.ofBytes(it).toString(), writer);
      writer.println();
      writer.println("Meta Blocks:");
      metaSizes.write(it -> ByteSize.ofBytes(it).toString(), writer);
      writer.println();
      writer.println("Original File Sizes:");
      fileSizes.write(it -> ByteSize.ofBytes(it).toString(), writer);
      writer.println();
      writer.flush();
    }

    if (!bytesByExtension.isEmpty()) {
      out.println("size by extension:");

      long remainingSize = blockSizes.sum();
      List<Map.Entry<String, AtomicLong>> sortedEntries =
          bytesByExtension.entrySet().stream()
              .sorted(
                  Map.Entry.<String, AtomicLong>comparingByValue(
                          Comparator.comparing(AtomicLong::get))
                      .reversed())
              .toList();
      for (int i = 0; i < 15 && i < sortedEntries.size(); i++) {
        out.printf(
            "%17s: %s\n",
            sortedEntries.get(i).getKey(), ByteSize.ofBytes(sortedEntries.get(i).getValue().get()));
        remainingSize -= sortedEntries.get(i).getValue().get();
      }
      if (remainingSize > 0) {
        out.printf("           others: %s\n", ByteSize.ofBytes(remainingSize));
      }
      out.println();
    }

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

  private static void trackBytesByExtension(String fileName, long fileSize, Map<String, AtomicLong> bytesByExtension) {
    int idx = fileName.lastIndexOf('.');
    if (idx >= 1 && idx < fileName.length() - 1 && idx >= fileName.length() - 6) {
      // Require at least one character before the last dot, and only consider extensions between 1 and 5 characters.
      String extension = fileName.substring(idx + 1).toLowerCase();
      bytesByExtension.computeIfAbsent(extension, unused -> new AtomicLong()).addAndGet(fileSize);
    } else {
      bytesByExtension.computeIfAbsent("<none>", unused -> new AtomicLong()).addAndGet(fileSize);
    }
  }

  private Gletscher.Directory fetchDirectory(PersistedBlock root) {
    try {
      return Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(root)));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}

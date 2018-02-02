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

package ws.moor.gletscher.commands;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.util.MoreArrays;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;

@Command(
  name = "spot_check",
  description =
      "Downloads a random sample from the backup and compares the bytes against live files locally."
)
class SpotCheckCommand extends AbstractCommand {

  static HashFunction hashFn = Hashing.hmacSha256(MoreArrays.randomBytes(new Random(), 32));

  SpotCheckCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    options.addOption(
        Option.builder()
            .longOpt("max_blocks")
            .hasArg()
            .type(Integer.class)
            .argName("BLOCK_COUNT")
            .build());
    options.addOption(
        Option.builder()
            .longOpt("max_bytes")
            .hasArg()
            .type(Long.class)
            .argName("BYTE_COUNT")
            .build());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    String blocksArg = commandLine.getOptionValue("max_blocks", "128");
    final int maxBlocksToCheck = Integer.valueOf(blocksArg);

    String bytesArg = commandLine.getOptionValue("max_bytes", "536870912");
    final long maxBytesToCheck = Long.valueOf(bytesArg);

    Optional<Catalog> catalog = catalogStore.getLatestCatalog();
    if (!catalog.isPresent()) {
      context.getStdErr().println("No existing backups found - nothing to verify.");
      return -1;
    }
    CatalogReader reader = new CatalogReader(blockStore, catalog.get());

    PriorityQueue<Sample> pq =
        new PriorityQueue<>(
            (o1, o2) -> Double.compare(o2.score, o1.score)); // highest score in front
    long totalSize = 0;
    Iterator<CatalogReader.FileInformation> it = reader.walk();
    while (it.hasNext()) {
      CatalogReader.FileInformation file = it.next();
      long offset = 0;
      for (PersistedBlock block : file.blockList) {
        Sample sample = new Sample(file.path, offset, block);

        if (pq.size() < maxBlocksToCheck || sample.score < pq.peek().score) {
          pq.add(sample);
          totalSize += sample.block.getOriginalLength();

          while (pq.size() > maxBlocksToCheck || totalSize > maxBytesToCheck) {
            Sample removedSample = pq.poll();
            totalSize -= removedSample.block.getOriginalLength();
          }
        }
        offset += block.getOriginalLength();
      }
    }

    boolean allGood = true;
    while (!pq.isEmpty()) {
      Sample sample = pq.poll();

      context
          .getStdOut()
          .printf(
              "checking %s (offset %d, %d bytes): ",
              sample.path, sample.offset, sample.block.getOriginalLength());

      try (SeekableByteChannel channel =
          Files.newByteChannel(sample.path, StandardOpenOption.READ)) {
        channel.position(sample.offset);

        ByteBuffer data = ByteBuffer.allocate(sample.block.getOriginalLength());
        while (data.remaining() > 0) {
          channel.read(data);
        }
        data.rewind();

        byte[] remoteData = Futures.getUnchecked(blockStore.retrieve(sample.block));
        ByteBuffer remoteBytes = ByteBuffer.wrap(remoteData);

        if (remoteBytes.equals(data)) {
          context.getStdOut().println("success.");
        } else {
          context.getStdOut().println("failed!");
          allGood = false;
        }
      }
    }

    return allGood ? 0 : -1;
  }

  private class Sample {

    private final double score;
    private final Path path;

    private final long offset;
    private final PersistedBlock block;

    Sample(Path path, long offset, PersistedBlock block) {
      double adjustment = block.getOriginalLength() > (1 << 20) ? 4.0 : 1.0;
      this.score =
          adjustment
              * (double) (hashFn.hashBytes(block.getSignature().asBytes()).asInt() & 0xfffffff)
              / 0xfffffff;
      this.path = path;
      this.offset = offset;
      this.block = block;
    }
  }
}

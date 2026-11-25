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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

@Command(name = "contains", description = "Checks whether local files are part of a backup.")
class ContainsCommand extends AbstractCommand {

  private static final int MAX_CONCURRENT_FILE_HASHES = 64;

  ContainsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    addSnapshotsOption(
        options, "Search at most the last N snapshots, or 'all' (default: 1).");
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (args.isEmpty()) {
      throw new InvalidUsageException(this, "Must provide at least one file name.");
    }

    int snapshotLimit = parseSnapshotsOption(this, commandLine, 1);

    List<InputFile> inputs = new ArrayList<>(args.size());
    for (String file : args) {
      Path path = context.getFileSystem().getPath(file);
      if (!Files.isRegularFile(path)) {
        context.getStdErr().printf("not a regular file: %s%n", file);
        return -1;
      }
      inputs.add(new InputFile(file, path));
    }

    List<List<PersistedBlock>> blocksByInput = hashFilesConcurrently(inputs);
    if (blocksByInput == null) {
      return -1;
    }

    List<InputFile> emptyFiles = new ArrayList<>();
    Map<List<PersistedBlock>, List<Integer>> pendingByBlocks = new HashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      List<PersistedBlock> blocks = blocksByInput.get(i);
      if (blocks.isEmpty()) {
        emptyFiles.add(inputs.get(i));
      } else {
        pendingByBlocks.computeIfAbsent(blocks, ignored -> new ArrayList<>()).add(i);
      }
    }

    MatchResult[] matchResults = new MatchResult[inputs.size()];
    if (!pendingByBlocks.isEmpty()) {
      searchSnapshots(snapshotLimit, pendingByBlocks, matchResults);
    }

    List<Integer> foundIndices = new ArrayList<>();
    List<InputFile> notFoundFiles = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      if (blocksByInput.get(i).isEmpty()) {
        continue;
      }
      if (matchResults[i] != null) {
        foundIndices.add(i);
      } else {
        notFoundFiles.add(inputs.get(i));
      }
    }

    printResults(inputs, emptyFiles, foundIndices, matchResults, notFoundFiles);
    return notFoundFiles.isEmpty() ? 0 : -1;
  }

  private List<List<PersistedBlock>> hashFilesConcurrently(List<InputFile> inputs)
      throws InterruptedException {
    StreamSplitter splitter = config.getStreamSplitter();
    SecretKeySpec signingKey = config.getSigningKey();

    Semaphore inFlight = new Semaphore(MAX_CONCURRENT_FILE_HASHES);
    List<ListenableFuture<List<PersistedBlock>>> futures = new ArrayList<>(inputs.size());
    for (InputFile input : inputs) {
      inFlight.acquire();
      ListenableFuture<List<PersistedBlock>> future =
          context.getExecutor().submit(() -> computeBlocks(input.path(), splitter, signingKey));
      future.addListener(inFlight::release, MoreExecutors.directExecutor());
      futures.add(future);
    }

    List<List<PersistedBlock>> results = new ArrayList<>(inputs.size());
    boolean failed = false;
    for (int i = 0; i < inputs.size(); i++) {
      try {
        results.add(futures.get(i).get());
      } catch (ExecutionException e) {
        context
            .getStdErr()
            .printf("failed to read %s: %s%n", inputs.get(i).displayName(), describeCause(e));
        failed = true;
      }
    }
    return failed ? null : results;
  }

  private List<PersistedBlock> computeBlocks(
      Path path, StreamSplitter splitter, SecretKeySpec signingKey) throws IOException {
    ImmutableList.Builder<PersistedBlock> blocks = ImmutableList.builder();
    Signer signer = new Signer(signingKey);
    try (InputStream is = context.readFile(path)) {
      Iterator<byte[]> it = splitter.split(is);
      while (it.hasNext()) {
        byte[] block = it.next();
        blocks.add(new PersistedBlock(signer.computeSignature(block), block.length));
      }
    }
    return blocks.build();
  }

  private void searchSnapshots(
      int snapshotLimit,
      Map<List<PersistedBlock>, List<Integer>> pendingByBlocks,
      MatchResult[] matchResults) {
    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(snapshotLimit);
    for (CloudFileStorage.FileHeader header : headers) {
      if (pendingByBlocks.isEmpty()) {
        break;
      }
      Catalog catalog = catalogStore.loadSnapshot(header).catalog();
      Map<List<PersistedBlock>, Set<String>> matchesInCatalog = new HashMap<>();

      CatalogReader catalogReader = CatalogReaders.fromBlockStore(blockStore, catalog);
      Iterator<CatalogReader.CatalogFile> it = catalogReader.walk();
      while (it.hasNext()) {
        CatalogReader.CatalogFile file = it.next();
        if (pendingByBlocks.containsKey(file.blockList)) {
          matchesInCatalog
              .computeIfAbsent(file.blockList, ignored -> new TreeSet<>())
              .add(file.path.getHumanReadableString());
        }
      }

      for (Map.Entry<List<PersistedBlock>, Set<String>> entry : matchesInCatalog.entrySet()) {
        List<Integer> matchedIndices = pendingByBlocks.remove(entry.getKey());
        MatchResult result =
            new MatchResult(catalog.getStartTime(), ImmutableList.copyOf(entry.getValue()));
        for (int idx : matchedIndices) {
          matchResults[idx] = result;
        }
      }
    }
  }

  private void printResults(
      List<InputFile> inputs,
      List<InputFile> emptyFiles,
      List<Integer> foundIndices,
      MatchResult[] matchResults,
      List<InputFile> notFoundFiles) {
    PrintStream out = context.getStdOut();
    DateTimeFormatter timestampFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z", Locale.US)
            .withZone(context.getClock().getZone());

    boolean printedSection = false;
    if (!emptyFiles.isEmpty()) {
      out.println("Empty (0 bytes):");
      for (InputFile file : emptyFiles) {
        out.printf("  %s%n", file.displayName());
      }
      printedSection = true;
    }

    if (!foundIndices.isEmpty()) {
      if (printedSection) {
        out.println();
      }
      out.println("Found:");
      for (int idx : foundIndices) {
        MatchResult match = matchResults[idx];
        out.printf(
            "  %s (in snapshot %s):%n",
            inputs.get(idx).displayName(), timestampFormatter.format(match.catalogStartTime()));
        for (String catalogPath : match.catalogPaths()) {
          out.printf("    %s%n", catalogPath);
        }
      }
      printedSection = true;
    }

    if (!notFoundFiles.isEmpty()) {
      if (printedSection) {
        out.println();
      }
      out.println("Not found:");
      for (InputFile file : notFoundFiles) {
        out.printf("  %s%n", file.displayName());
      }
    }
  }

  private static String describeCause(ExecutionException e) {
    Throwable cause = e.getCause();
    return cause != null ? cause.toString() : e.toString();
  }

  private record InputFile(String displayName, Path path) {}

  private record MatchResult(Instant catalogStartTime, ImmutableList<String> catalogPaths) {}
}

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

package ws.moor.gletscher.commands;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.StreamSplitter;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

@Command(name = "backup", description = "Backup local files remotely.")
class BackupCommand extends AbstractCommand {
  BackupCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept any arguments.");
    }

    for (Path dir : config.getIncludes()) {
      if (!Files.isDirectory(dir)) {
        context.getStdErr().printf("not a directory: %s\n", dir);
        return -1;
      }
      if (!Files.isReadable(dir)) {
        context.getStdErr().printf("can't read directory: %s\n", dir);
        return -1;
      }
    }

    StreamSplitter splitter = config.getStreamSplitter();
    Optional<Catalog> latestCatalog = catalogStore.getLatestCatalog();
    @Nullable CatalogReader catalogReader =
        latestCatalog.isPresent() ? new CatalogReader(blockStore, latestCatalog.get()) : null;

    Instant startTime = context.getClock().instant();
    FileSystemReader fileSystemReader = new FileSystemReader(config.getIncludes(), context.getStdErr());
    BackUpper backUpper = new BackUpper(
        catalogReader, splitter, blockStore, config.getExcludes(), context.getStdOut(), context.getStdErr(), context.getClock());
    Map<Path, PersistedBlock> roots = getUnchecked(fileSystemReader.start(backUpper));

    Instant endTime = context.getClock().instant();
    Catalog catalog = Catalog.fromNewBackup(startTime, endTime, roots);
    catalogStore.store(catalog);
    context.getStdOut().println("new catalog: " + catalog);
    return 0;
  }

  private Map<Path, PersistedBlock> getUnchecked(Map<Path, ListenableFuture<PersistedBlock>> rootFutures) {
    Map<Path, PersistedBlock> roots = new TreeMap<>();
    for (Map.Entry<Path, ListenableFuture<PersistedBlock>> entry : rootFutures.entrySet()) {
      roots.put(entry.getKey(), Futures.getUnchecked(entry.getValue()));
    }
    return roots;
  }

  private static class BackUpper implements FileSystemReader.Visitor<ListenableFuture<PersistedBlock>> {

    @Nullable private final CatalogReader catalogReader;
    private final StreamSplitter splitter;
    private final BlockStore blockStore;
    private final Set<Pattern> skipPatterns;
    private final PrintStream stdout;
    private final PrintStream stderr;
    private final Clock clock;
    private final ListeningExecutorService mainWorkerPool;
    private final Semaphore outstandingStoreRequests;

    BackUpper(@Nullable CatalogReader catalogReader, StreamSplitter splitter, BlockStore blockStore,
              Set<Pattern> skipPatterns, PrintStream stdout, PrintStream stderr, Clock clock) {
      this.catalogReader = catalogReader;
      this.splitter = splitter;
      this.blockStore = blockStore;
      this.skipPatterns = skipPatterns;
      this.stdout = stdout;
      this.stderr = stderr;
      this.clock = clock;
      this.mainWorkerPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
      this.outstandingStoreRequests = new Semaphore(8);
    }

    @Override
    public ListenableFuture<PersistedBlock> visit(
        Path directory, List<FileSystemReader.Entry> entries, FileSystemReader.Recursor<ListenableFuture<PersistedBlock>> recursor) {
      @Nullable CatalogReader.DirectoryInformation existingDirectory =
          catalogReader != null ? catalogReader.findDirectory(directory) : null;

      Gletscher.Directory.Builder dirProtoBuilder = Gletscher.Directory.newBuilder();
      dirProtoBuilder.setStartTimeMillis(clock.millis());
      if (existingDirectory != null) {
        dirProtoBuilder.setPreviousVersion(existingDirectory.getAddress().toProto());
      }

      SortedMap<Path, ListenableFuture<Gletscher.DirectoryEntry>> entryMap = new TreeMap<>();
      for (FileSystemReader.Entry entry : entries) {
        if (isSkippedPath(entry.path)) {
          stdout.println("skipping: " + entry.path);
          continue;
        }

        if (entry.isRegularFile()) {
          @Nullable CatalogReader.FileInformation existingFile =
              existingDirectory != null ? existingDirectory.findFileInformation(entry.path) : null;
          entryMap.put(entry.path, handleRegularFile(entry, existingFile));
        } else if (entry.isSymbolicLink()) {
          entryMap.put(entry.path, handleSymbolicLink(entry));
        } else if (entry.isDirectory()) {
          ListenableFuture<PersistedBlock> childDirectory = recursor.recurse(entry.path);
          entryMap.put(entry.path, Futures.transform(childDirectory, childBlock -> {
            Gletscher.ChildDirectoryEntry.Builder childDirBuilder = Gletscher.ChildDirectoryEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setBlock(childBlock.toProto());
            return Gletscher.DirectoryEntry.newBuilder().setDirectory(childDirBuilder).build();
          }));
        } else {
          stderr.printf("skipping unknown file type: %s\n", entry.path);
        }
      }

      return Futures.whenAllComplete(entryMap.values()).callAsync(new AsyncCallable<PersistedBlock>() {
        @Override public ListenableFuture<PersistedBlock> call() throws Exception {
          for (Map.Entry<Path, ListenableFuture<Gletscher.DirectoryEntry>> entry : entryMap.entrySet()) {
            try {
              dirProtoBuilder.addEntry(Futures.getDone(entry.getValue()));
            } catch (ExecutionException e) {
              stderr.printf("failed to read path: %s\n", entry.getKey());
            }
          }

          dirProtoBuilder.setEndTimeMillis(clock.millis());
          Gletscher.Directory dirProto = dirProtoBuilder.build();

          if (existingDirectory != null && !existingDirectory.hasChanged(dirProto)) {
            return Futures.immediateFuture(existingDirectory.getAddress());
          }
          return storeThrottled(dirProto.toByteArray(), true);
        }
      }, mainWorkerPool);
    }

    private ListenableFuture<Gletscher.DirectoryEntry> handleRegularFile(
        FileSystemReader.Entry entry, CatalogReader.FileInformation existingFile) {
      Instant currentLastModifiedTime = entry.attributes.lastModifiedTime().toInstant();
      if (existingFile == null
          || !existingFile.lastModifiedTime.equals(currentLastModifiedTime)
          || existingFile.getOriginalSize() != entry.attributes.size()) {
        if (existingFile == null) {
          stdout.println("new file: " + entry.path);
        } else {
          stdout.println("changed file: " + entry.path);
        }
        ListenableFuture<List<PersistedBlock>> contentsFuture = uploadFileContents(entry.path);
        return Futures.transform(contentsFuture, new Function<List<PersistedBlock>, Gletscher.DirectoryEntry>() {
          @Override public Gletscher.DirectoryEntry apply(List<PersistedBlock> input) {
            Gletscher.FileEntry.Builder fileBuilder = Gletscher.FileEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setLastModifiedMillis(currentLastModifiedTime.toEpochMilli());
            for (PersistedBlock block : input) {
              fileBuilder.addBlock(block.toProto());
            }
            return Gletscher.DirectoryEntry.newBuilder().setFile(fileBuilder).build();
          }
        });
      } else {
        // matching
        Gletscher.FileEntry.Builder fileBuilder = Gletscher.FileEntry.newBuilder()
            .setName(entry.path.getFileName().toString())
            .setLastModifiedMillis(existingFile.lastModifiedTime.toEpochMilli());
        for (PersistedBlock block : existingFile.blockList) {
          fileBuilder.addBlock(block.toProto());
        }
        return Futures.immediateFuture(Gletscher.DirectoryEntry.newBuilder().setFile(fileBuilder).build());
      }
    }

    private ListenableFuture<List<PersistedBlock>> uploadFileContents(Path path) {
      ListenableFuture<List<ListenableFuture<PersistedBlock>>> future = mainWorkerPool.submit(() -> {
        List<ListenableFuture<PersistedBlock>> futures = new ArrayList<>();
        Iterator<byte[]> parts = splitter.split(Files.newInputStream(path));
        while (parts.hasNext()) {
          byte[] part = parts.next();
          futures.add(storeThrottled(part, false));
        }
        return futures;
      });
      return Futures.transformAsync(future, Futures::allAsList);
    }

    private ListenableFuture<Gletscher.DirectoryEntry> handleSymbolicLink(FileSystemReader.Entry entry) {
      return mainWorkerPool.submit(() -> {
        Gletscher.SymLinkEntry.Builder symlinkBuilder = Gletscher.SymLinkEntry.newBuilder()
            .setName(entry.path.getFileName().toString())
            .setTarget(Files.readSymbolicLink(entry.path).toString());
        return Gletscher.DirectoryEntry.newBuilder().setSymlink(symlinkBuilder).build();
      });
    }

    private ListenableFuture<PersistedBlock> storeThrottled(byte[] part, boolean cache) {
      outstandingStoreRequests.acquireUninterruptibly();
      ListenableFuture<PersistedBlock> future = blockStore.store(part, cache);
      Futures.addCallback(future, new FutureCallback<PersistedBlock>() {
        @Override public void onSuccess(@Nullable PersistedBlock result) {
          outstandingStoreRequests.release();
        }
        @Override public void onFailure(Throwable t) {
          outstandingStoreRequests.release();
        }
      });
      return future;
    }

    private boolean isSkippedPath(Path path) {
      for (Pattern pattern : skipPatterns) {
        if (pattern.matcher(path.toString()).find()) {
          return true;
        }
      }
      return false;
    }
  }
}

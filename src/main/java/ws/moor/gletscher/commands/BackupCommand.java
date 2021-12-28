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

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogPath;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.StreamSplitter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
    CatalogReader catalogReader = latestCatalog.map(c -> CatalogReaders.fromBlockStore(blockStore, c)).orElse(CatalogReaders.empty());

    Instant startTime = context.getClock().instant();
    BackupObserver observer = new BackupObserver(context.getStdOut(), context.getStdErr());
    FileSystemReader fileSystemReader =
        new FileSystemReader(config.getIncludes(), observer);
    BackUpper backUpper =
        new BackUpper(
            catalogReader,
            splitter,
            blockStore,
            config.getExcludes(),
            observer,
            context.getClock(),
            context);
    Map<Path, PersistedBlock> roots = getUnchecked(fileSystemReader.start(backUpper));

    Instant endTime = context.getClock().instant();
    Catalog catalog = Catalog.fromNewBackup(startTime, endTime, roots, latestCatalog.orElse(null));
    PersistedBlock pb = catalogStore.store(catalog);
    context.getStdOut().println("new catalog: " + pb.getSignature());
    return 0;
  }

  private Map<Path, PersistedBlock> getUnchecked(
      Map<Path, ListenableFuture<PersistedBlock>> rootFutures) {
    Map<Path, PersistedBlock> roots = new TreeMap<>();
    for (Map.Entry<Path, ListenableFuture<PersistedBlock>> entry : rootFutures.entrySet()) {
      roots.put(entry.getKey(), Futures.getUnchecked(entry.getValue()));
    }
    return roots;
  }

  private static class BackupObserver implements FileSystemReader.Observer {

    private PrintStream stdout;
    private PrintStream stderr;

    BackupObserver(PrintStream stdout, PrintStream stderr) {
      this.stdout = stdout;
      this.stderr = stderr;
    }

    void skipping(FileSystemReader.Entry entry) {
      stdout.printf("skipping: %s\n", entry.path);
    }

    void skippingUnknownFileType(FileSystemReader.Entry entry) {
      stderr.printf("skipping unknown file type: %s\n", entry.path);
    }

    void readFailure(Path path, Throwable cause) {
      stderr.printf("failed to read path: %s: %s\n", path, cause.toString());
    }

    void newFile(FileSystemReader.Entry entry) {
      stdout.printf("new file: %s\n", entry.path);
    }

    void changedFile(CatalogReader.CatalogFile oldFile, FileSystemReader.Entry newFile) {
      stdout.printf("changed file: %s\n", newFile.path);
    }

    @Override
    public void unreadableDirectory(Path path) {
      stderr.printf("unreadable directory: %s\n", path);
    }

    @Override
    public void directoryListingError(Path path, Exception e) {
      stderr.printf("error while reading directory %s: %s\n", path, e);
    }
  }

  private static class BackUpper
      implements FileSystemReader.Visitor<ListenableFuture<PersistedBlock>> {

    private final CatalogReader catalogReader;
    private final StreamSplitter splitter;
    private final BlockStore blockStore;
    private final Set<Pattern> skipPatterns;
    private final BackupObserver observer;
    private final Clock clock;
    private final Semaphore pendingStoreRequests;
    private final Semaphore pendingStoreBytes;
    private final CommandContext context;
    private final Executor executor = Executors.newSingleThreadExecutor();

    BackUpper(
        CatalogReader catalogReader,
        StreamSplitter splitter,
        BlockStore blockStore,
        Set<Pattern> skipPatterns,
        BackupObserver observer,
        Clock clock,
        CommandContext context) {
      this.catalogReader = catalogReader;
      this.splitter = splitter;
      this.blockStore = blockStore;
      this.skipPatterns = skipPatterns;
      this.observer = observer;
      this.clock = clock;
      this.pendingStoreRequests = new Semaphore(64);
      this.pendingStoreBytes = new Semaphore(64 << 20);
      this.context = context;
    }

    @Override
    public ListenableFuture<PersistedBlock> visit(
        Path directory,
        List<FileSystemReader.Entry> entries,
        FileSystemReader.Recursor<ListenableFuture<PersistedBlock>> recursor) {
      @Nullable
      CatalogReader.CatalogDirectory existingDirectory = catalogReader.findDirectory(CatalogPath.fromLocalPath(directory));

      Gletscher.Directory.Builder dirProtoBuilder = Gletscher.Directory.newBuilder();
      dirProtoBuilder.setStartTimeMillis(clock.millis());
      if (existingDirectory != null) {
        dirProtoBuilder.setPreviousVersion(existingDirectory.getAddress().toProto());
      }

      SortedMap<Path, ListenableFuture<Gletscher.DirectoryEntry>> entryMap = new TreeMap<>();
      for (FileSystemReader.Entry entry : entries) {
        if (isSkippedPath(entry.path)) {
          observer.skipping(entry);
          continue;
        }

        if (entry.isRegularFile()) {
          @Nullable
          CatalogReader.CatalogFile existingFile =
              existingDirectory != null
                  ? existingDirectory.findFileInformation(entry.path.getFileName().toString())
                  : null;
          entryMap.put(entry.path, handleRegularFile(entry, existingFile));
        } else if (entry.isSymbolicLink()) {
          entryMap.put(entry.path, handleSymbolicLink(entry));
        } else if (entry.isDirectory()) {
          ListenableFuture<PersistedBlock> childDirectory = recursor.recurse(entry.path);
          entryMap.put(
              entry.path,
              Futures.transform(
                  childDirectory,
                  childBlock -> {
                    Gletscher.ChildDirectoryEntry.Builder childDirBuilder =
                        Gletscher.ChildDirectoryEntry.newBuilder()
                            .setName(entry.path.getFileName().toString())
                            .setBlock(childBlock.toProto());
                    return Gletscher.DirectoryEntry.newBuilder()
                        .setDirectory(childDirBuilder)
                        .build();
                  },
                  MoreExecutors.directExecutor()));
        } else {
          observer.skippingUnknownFileType(entry);
        }
      }

      return Futures.whenAllComplete(entryMap.values())
          .callAsync(
              new AsyncCallable<PersistedBlock>() {
                @Override
                public ListenableFuture<PersistedBlock> call() {
                  for (Map.Entry<Path, ListenableFuture<Gletscher.DirectoryEntry>> entry :
                      entryMap.entrySet()) {
                    try {
                      dirProtoBuilder.addEntry(Futures.getDone(entry.getValue()));
                    } catch (ExecutionException e) {
                      observer.readFailure(entry.getKey(), e.getCause());
                    }
                  }

                  dirProtoBuilder.setEndTimeMillis(clock.millis());
                  Gletscher.Directory dirProto = dirProtoBuilder.build();

                  if (!hasBeenModified(existingDirectory, dirProto)) {
                    return Futures.immediateFuture(existingDirectory.getAddress());
                  }
                  return storeThrottled(dirProto.toByteArray(), /*cache=*/true);
                }
              },
              executor);
    }

    private static boolean hasBeenModified(@Nullable CatalogReader.CatalogDirectory existingDirectory, Gletscher.Directory newProto) {
      if (existingDirectory == null) {
        return true;
      }

      Gletscher.Directory oldProto = existingDirectory.getProto();
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

    private ListenableFuture<Gletscher.DirectoryEntry> handleRegularFile(
        FileSystemReader.Entry entry, CatalogReader.CatalogFile existingFile) {
      Instant currentLastModifiedTime = entry.attributes.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.MILLIS);
      if (existingFile == null
          || !existingFile.lastModifiedTime.equals(currentLastModifiedTime)
          || existingFile.getOriginalSize() != entry.attributes.size()) {
        if (existingFile == null) {
          observer.newFile(entry);
        } else {
          observer.changedFile(existingFile, entry);
        }
        ListenableFuture<List<PersistedBlock>> contentsFuture = uploadFileContents(entry.path);
        return Futures.transform(
            contentsFuture,
            new Function<List<PersistedBlock>, Gletscher.DirectoryEntry>() {
              @Override
              public Gletscher.DirectoryEntry apply(List<PersistedBlock> input) {
                Gletscher.FileEntry.Builder fileBuilder =
                    Gletscher.FileEntry.newBuilder()
                        .setName(entry.path.getFileName().toString())
                        .setLastModifiedMillis(currentLastModifiedTime.toEpochMilli());
                for (PersistedBlock block : input) {
                  fileBuilder.addBlock(block.toProto());
                }
                return Gletscher.DirectoryEntry.newBuilder().setFile(fileBuilder).build();
              }
            },
            MoreExecutors.directExecutor());
      } else {
        // matching
        Gletscher.FileEntry.Builder fileBuilder =
            Gletscher.FileEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setLastModifiedMillis(existingFile.lastModifiedTime.toEpochMilli());
        for (PersistedBlock block : existingFile.blockList) {
          fileBuilder.addBlock(block.toProto());
        }
        return Futures.immediateFuture(
            Gletscher.DirectoryEntry.newBuilder().setFile(fileBuilder).build());
      }
    }

    private ListenableFuture<List<PersistedBlock>> uploadFileContents(Path path) {
      List<ListenableFuture<PersistedBlock>> futures = new ArrayList<>();
      try (InputStream is = context.readFile(path)) {
        Iterator<byte[]> parts = splitter.split(is);
        while (parts.hasNext()) {
          byte[] part = parts.next();
          futures.add(storeThrottled(part, false));
        }
      } catch (IOException | RuntimeException e) {
        return Futures.immediateFailedFuture(e);
      }
      return Futures.allAsList(futures);
    }

    private ListenableFuture<Gletscher.DirectoryEntry> handleSymbolicLink(
        FileSystemReader.Entry entry) {
      try {
        Gletscher.SymLinkEntry.Builder symlinkBuilder =
            Gletscher.SymLinkEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setTarget(Files.readSymbolicLink(entry.path).toString());
        return Futures.immediateFuture(
            Gletscher.DirectoryEntry.newBuilder().setSymlink(symlinkBuilder).build());
      } catch (IOException e) {
        return Futures.immediateFailedFuture(e);
      }
    }

    private ListenableFuture<PersistedBlock> storeThrottled(byte[] part, boolean cache) {
      pendingStoreRequests.acquireUninterruptibly();
      pendingStoreBytes.acquireUninterruptibly(part.length);
      ListenableFuture<PersistedBlock> future = blockStore.store(part, cache);
      releaseWhenDone(future, pendingStoreBytes, part.length);
      releaseWhenDone(future, pendingStoreRequests, 1);
      return future;
    }

    private static void releaseWhenDone(ListenableFuture<?> future, Semaphore semaphore, int permits) {
      future.addListener(() -> semaphore.release(permits), MoreExecutors.directExecutor());
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

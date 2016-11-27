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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    FileSystemReader<PersistedBlock> fileSystemReader = new FileSystemReader<>(config.getIncludes(), context.getStdErr());
    BackUpper backUpper = new BackUpper(
        catalogReader, splitter, blockStore, config.getExcludes(), context.getStdOut(), context.getStdErr(), context.getClock());
    Map<Path, PersistedBlock> roots = fileSystemReader.start(backUpper);

    Instant endTime = context.getClock().instant();
    Catalog catalog = Catalog.fromNewBackup(startTime, endTime, roots);
    catalogStore.store(catalog);
    context.getStdOut().println("new catalog: " + catalog);
    return 0;
  }

  private static class BackUpper implements FileSystemReader.Visitor<PersistedBlock> {

    @Nullable private final CatalogReader catalogReader;
    private final StreamSplitter splitter;
    private final BlockStore blockStore;
    private final Set<Pattern> skipPatterns;
    private final PrintStream stdout;
    private final PrintStream stderr;
    private final Clock clock;

    BackUpper(@Nullable CatalogReader catalogReader, StreamSplitter splitter, BlockStore blockStore,
              Set<Pattern> skipPatterns, PrintStream stdout, PrintStream stderr, Clock clock) {
      this.catalogReader = catalogReader;
      this.splitter = splitter;
      this.blockStore = blockStore;
      this.skipPatterns = skipPatterns;
      this.stdout = stdout;
      this.stderr = stderr;
      this.clock = clock;
    }

    @Override
    public PersistedBlock visit(Path directory, List<FileSystemReader.Entry> entries, FileSystemReader.Recursor<PersistedBlock> recursor) {
      @Nullable CatalogReader.DirectoryInformation existingDirectory =
          catalogReader != null ? catalogReader.findDirectory(directory) : null;

      Gletscher.Directory.Builder dirProtoBuilder = Gletscher.Directory.newBuilder();
      dirProtoBuilder.setStartTimeMillis(clock.millis());

      for (FileSystemReader.Entry entry : entries) {
        if (isSkippedPath(entry.path)) {
          stdout.println("skipping: " + entry.path);
          continue;
        }

        if (entry.isRegularFile()) {
          Instant currentLastModifiedTime = entry.attributes.lastModifiedTime().toInstant();

          @Nullable CatalogReader.FileInformation existingFile =
              existingDirectory != null ? existingDirectory.findFileInformation(entry.path) : null;
          if (existingFile == null || !existingFile.lastModifiedTime.equals(currentLastModifiedTime)) {
            if (existingFile == null) {
              stdout.println("new file: " + entry.path);
            } else {
              stdout.println("changed file: " + entry.path);
            }

            Gletscher.FileEntry.Builder fileBuilder = Gletscher.FileEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setLastModifiedMillis(currentLastModifiedTime.toEpochMilli());
            try {
              Iterator<byte[]> parts = splitter.split(Files.newInputStream(entry.path));
              while (parts.hasNext()) {
                byte[] part = parts.next();
                ListenableFuture<PersistedBlock> persistedBlock = blockStore.store(part, false);
                fileBuilder.addBlock(Futures.getUnchecked(persistedBlock).toProto());
              }
              dirProtoBuilder.addEntryBuilder().setFile(fileBuilder);
            } catch (IOException e) {
              stderr.println("failed to read file: " + entry.path);
            }
          } else {
            // matching
            Gletscher.FileEntry.Builder fileBuilder = Gletscher.FileEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setLastModifiedMillis(existingFile.lastModifiedTime.toEpochMilli());
            for (PersistedBlock block : existingFile.blockList) {
              fileBuilder.addBlock(block.toProto());
            }
            dirProtoBuilder.addEntryBuilder().setFile(fileBuilder);
          }
        } else if (entry.isSymbolicLink()) {
          try {
            Gletscher.SymLinkEntry.Builder symlinkBuilder = Gletscher.SymLinkEntry.newBuilder()
                .setName(entry.path.getFileName().toString())
                .setTarget(Files.readSymbolicLink(entry.path).toString());
            dirProtoBuilder.addEntryBuilder().setSymlink(symlinkBuilder);
          } catch (IOException e) {
            stderr.println("couldn't resolve symlink: " + entry.path);
          }
        } else if (entry.isDirectory()) {
          Gletscher.ChildDirectoryEntry.Builder childDirBuilder = Gletscher.ChildDirectoryEntry.newBuilder()
              .setName(entry.path.getFileName().toString())
              .setBlock(recursor.recurse(entry.path).toProto());
          dirProtoBuilder.addEntryBuilder().setDirectory(childDirBuilder);
        } else {
          stderr.printf("skipping unknown file type: %s\n", entry.path);
        }
      }

      if (existingDirectory != null) {
        dirProtoBuilder.setPreviousVersion(existingDirectory.getAddress().toProto());
      }
      dirProtoBuilder.setEndTimeMillis(clock.millis());
      Gletscher.Directory dirProto = dirProtoBuilder.build();

      if (existingDirectory != null && !existingDirectory.hasChanged(dirProto)) {
        return existingDirectory.getAddress();
      }
      return Futures.getUnchecked(blockStore.store(dirProto.toByteArray(), true));
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

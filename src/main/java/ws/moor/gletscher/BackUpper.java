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

package ws.moor.gletscher;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.RootCatalogReader;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.StreamSplitter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class BackUpper implements FileSystemReader.Visitor<PersistedBlock> {

  private final CatalogReader catalogReader;
  private final StreamSplitter splitter;
  private final BlockStore blockStore;
  private final Set<Pattern> skipPatterns;
  private final PrintStream stdout;
  private final PrintStream stderr;

  public BackUpper(CatalogReader catalogReader, StreamSplitter splitter, BlockStore blockStore,
                   Set<Pattern> skipPatterns, PrintStream stdout, PrintStream stderr) {
    this.catalogReader = catalogReader;
    this.splitter = splitter;
    this.blockStore = blockStore;
    this.skipPatterns = skipPatterns;
    this.stdout = stdout;
    this.stderr = stderr;
  }

  @Override
  public PersistedBlock visit(Path directory, List<FileSystemReader.Entry> entries, FileSystemReader.Recursor<PersistedBlock> recursor) {
    Gletscher.Directory.Builder dirProtoBuilder = Gletscher.Directory.newBuilder();

    for (FileSystemReader.Entry entry : entries) {
      if (isSkippedPath(entry.path)) {
        stdout.println("skipping: " + entry.path);
        continue;
      }

      if (entry.isRegularFile()) {
        Instant currentLastModifiedTime = entry.attributes.lastModifiedTime().toInstant();

        RootCatalogReader.FileInformation existingFile = catalogReader.findFile(entry.path);
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
              ListenableFuture<PersistedBlock> persistedBlock = blockStore.store(part);
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

    Gletscher.Directory dirProto = dirProtoBuilder.build();
    return Futures.getUnchecked(blockStore.store(dirProto.toByteArray()));
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

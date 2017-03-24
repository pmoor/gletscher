/*
 * Copyright 2017 Patrick Moor <patrick@moor.ws>
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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.proto.Gletscher;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.List;

@Command(name = "restore", description = "Restore a backup locally.")
class RestoreCommand extends AbstractCommand {
  RestoreCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (args.size() != 1) {
      throw new InvalidUsageException(this, "Must provide a restore directory.");
    }

    Path restoreRoot = context.getFileSystem().getPath(args.get(0));
    if (Files.exists(restoreRoot, LinkOption.NOFOLLOW_LINKS)) {
      context.getStdErr().println("Restore directory cannot exist yet.");
      return -1;
    }
    Files.createDirectories(restoreRoot);

    List<Catalog> lastCatalogs = catalogStore.findLastCatalogs(1);
    if (lastCatalogs.isEmpty()) {
      context.getStdErr().println("no existing backup found");
      return -1;
    }

    Catalog catalog = Iterables.getOnlyElement(lastCatalogs);
    Gletscher.Directory rootDir = Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(catalog.getOnlyRootBlock())));
    restoreInner(blockStore, rootDir, restoreRoot);
    return 0;
  }

  private void restoreInner(BlockStore blockStore, Gletscher.Directory dir, Path path) throws IOException {
    for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
      switch (entry.getTypeCase()) {
        case FILE:
          Path tmpFile = Files.createTempFile(path, ".gletscher-", ".tmprestore");
          OutputStream fos = new BufferedOutputStream(Files.newOutputStream(tmpFile, StandardOpenOption.TRUNCATE_EXISTING));
          for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
            byte[] data = Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(block)));
            fos.write(data);
          }
          fos.close();
          Files.setLastModifiedTime(tmpFile, FileTime.fromMillis(entry.getFile().getLastModifiedMillis()));

          Path actualFile = path.resolve(entry.getFile().getName());
          Files.move(tmpFile, actualFile);
          break;
        case DIRECTORY:
          Path childPath = path.resolve(entry.getDirectory().getName());
          Gletscher.Directory childDir = Gletscher.Directory.parseFrom(
              Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(entry.getDirectory().getBlock()))));
          Files.createDirectory(childPath);
          restoreInner(blockStore, childDir, childPath);
          break;
        case SYMLINK:
          Files.createSymbolicLink(path.resolve(entry.getSymlink().getName()),
              path.getFileSystem().getPath(entry.getSymlink().getTarget()));
          break;
        default:
          throw new IllegalArgumentException(entry.toString());
      }
    }
  }
}

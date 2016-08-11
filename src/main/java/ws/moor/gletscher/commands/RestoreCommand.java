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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Signer;

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

    Configuration config = loadConfig(commandLine);
    CloudFileStorage cloudFileStorage = buildCloudFileStorage(config);
    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(cloudFileStorage, context.getClock());

    List<Gletscher.BackupRecord> lastBackups = catalogStore.findLastBackups(1);
    if (lastBackups.isEmpty()) {
      context.getStdErr().println("no existing backup found");
      return -1;
    }

    PersistedBlock root = PersistedBlock.fromProto(lastBackups.get(0).getRootDirectory());
    Gletscher.Directory rootDir = Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(root)));
    restoreInner(blockStore, rootDir, restoreRoot);
    return 0;
  }

  private void restoreInner(BlockStore blockStore, Gletscher.Directory dir, Path path) throws IOException {
    for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
      switch (entry.getTypeCase()) {
        case FILE:
          Path actualFile = path.resolve(entry.getFile().getName());
          OutputStream fos = new BufferedOutputStream(Files.newOutputStream(actualFile, StandardOpenOption.CREATE_NEW));
          for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
            byte[] data = Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(block)));
            fos.write(data);
          }
          fos.close();
          Files.setLastModifiedTime(actualFile, FileTime.fromMillis(entry.getFile().getLastModifiedMillis()));
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

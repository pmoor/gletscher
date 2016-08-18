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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.BackUpper;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.catalog.CompositeCatalogReader;
import ws.moor.gletscher.catalog.RootCatalogReader;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    Configuration config = loadConfig(commandLine);
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

    CloudFileStorage cloudFileStorage = buildCloudFileStorage(config);
    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(context.getFileSystem(), cloudFileStorage);

    StreamSplitter splitter = config.getStreamSplitter();
    List<CatalogReader> readers = new ArrayList<>();
    for (Catalog catalog : catalogStore.findLastCatalogs(10)) {
      readers.add(new RootCatalogReader(blockStore, catalog));
    }
    CatalogReader catalogReader = new CompositeCatalogReader(readers);

    Instant startTime = context.getClock().instant();
    FileSystemReader<PersistedBlock> fileSystemReader = new FileSystemReader<>(config.getIncludes(), context.getStdErr());
    BackUpper backUpper = new BackUpper(catalogReader, splitter, blockStore, config.getExcludes(), context.getStdOut(), context.getStdErr());
    Map<Path, PersistedBlock> roots = fileSystemReader.start(backUpper);

    Instant endTime = context.getClock().instant();
    Catalog catalog = Catalog.fromNewBackup(startTime, endTime, roots);
    catalogStore.store(catalog);
    context.getStdOut().println("new catalog: " + catalog);
    return 0;
  }
}

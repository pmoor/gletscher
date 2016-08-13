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
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogAnalyzer;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Signer;

import java.util.List;

@Command(name = "stats", description = "Print size stats of the last 3 remote catalogs.")
class StatsCommand extends AbstractCommand {
  StatsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    Configuration config = loadConfig(commandLine);

    CloudFileStorage cloudFileStorage = buildCloudFileStorage(config);
    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogAnalyzer analyzer = new CatalogAnalyzer(blockStore);
    CatalogStore catalogStore = new CatalogStore(cloudFileStorage, context.getClock());

    List<Gletscher.BackupRecord> lastBackups = catalogStore.findLastBackups(3);
    for (Gletscher.BackupRecord backup : lastBackups) {
      PersistedBlock root = PersistedBlock.fromProto(backup.getRootDirectory());
      analyzer.analyze(root, context.getStdOut());
    }

    return 0;
  }
}

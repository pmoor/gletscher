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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.catalog.CatalogAnalyzer;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;

import java.util.List;

@Command(name = "stats", description = "Print size stats of the last N remote snapshots (default: 1).")
class StatsCommand extends AbstractCommand {
  StatsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    addSnapshotsOption(
        options,
        "Analyze the last N snapshots, or 'all' (default: 1).");
    options.addOption(
        Option.builder("v")
            .longOpt("verbose")
            .desc("Print more verbose statistics, including histograms.")
            .get());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    int snapshotLimit = parseSnapshotsOption(this, commandLine, 1);

    CatalogAnalyzer analyzer =
        new CatalogAnalyzer(blockStore, /* printHistograms= */ commandLine.hasOption('v'));
    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(snapshotLimit);
    for (CloudFileStorage.FileHeader header : headers) {
      CatalogStore.Snapshot snapshot = catalogStore.loadSnapshot(header);
      analyzer.analyze(snapshot, context.getStdOut());
    }
    analyzer.printSummary(context.getStdOut());
    return 0;
  }
}

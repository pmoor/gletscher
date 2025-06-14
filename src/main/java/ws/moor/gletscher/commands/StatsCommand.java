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
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogAnalyzer;

import java.util.List;
import java.util.Optional;

@Command(name = "stats", description = "Print size stats of the last 3 remote catalogs.")
class StatsCommand extends AbstractCommand {
  StatsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    options.addOption(
        Option.builder("v")
            .longOpt("verbose")
            .desc("Print more verbose statistics, including histograms.")
            .build());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    CatalogAnalyzer analyzer = new CatalogAnalyzer(blockStore, /* printHistograms= */commandLine.hasOption('v'));
    Optional<Catalog> catalog = catalogStore.getLatestCatalog();
    while (catalog.isPresent()) {
      analyzer.analyze(catalog.get(), context.getStdOut());
      catalog = catalog.get().getBaseCatalog().map(catalogStore::load);
    }
    return 0;
  }
}

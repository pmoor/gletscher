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
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.catalog.RootCatalogReader;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.util.Signer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

@Command(name = "search", description = "Look for a file name in a remote backup.")
class SearchCommand extends AbstractCommand {
  SearchCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    Configuration config = loadConfig(commandLine);
    CloudFileStorage cloudFileStorage = buildCloudFileStorage(config);
    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(context.getFileSystem(), cloudFileStorage);

    List<Pattern> positive = new ArrayList<>();
    List<Pattern> negative = new ArrayList<>();
    for (String arg : args) {
      boolean isNegative = false;
      if (arg.startsWith("-")) {
        arg = arg.substring(1);
        isNegative = true;
      }
      Pattern pattern = Pattern.compile(arg);
      (isNegative ? negative : positive).add(pattern);
    }

    for (Catalog catalog : catalogStore.findLastCatalogs(16)) {
      context.getStdOut().printf("Backup %s:\n", catalog);

      RootCatalogReader reader = new RootCatalogReader(blockStore, catalog);
      Iterator<CatalogReader.FileInformation> iterator = reader.walk();
      while (iterator.hasNext()) {
        CatalogReader.FileInformation file = iterator.next();
        boolean matches = true;
        for (Pattern pattern : positive) {
          if (!pattern.matcher(file.path.toString()).find()) {
            matches = false;
            break;
          }
        }
        if (matches) {
          for (Pattern pattern : negative) {
            if (pattern.matcher(file.path.toString()).find()) {
              matches = false;
              break;
            }
          }
        }
        if (matches) {
          context.getStdOut().printf("\t%s: %s (%d bytes)\n", file.path, file.lastModifiedTime, file.getOriginalSize());
        }
      }
      context.getStdOut().println();
    }

    return 0;
  }
}

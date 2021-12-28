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
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Command(
    name = "verify",
    description = "Verifies that a catalog has all its data stored in the cloud."
)
class VerifyCommand extends AbstractCommand {
  VerifyCommand(CommandContext context) {
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

    Optional<Catalog> catalog = catalogStore.getLatestCatalog();
    if (!catalog.isPresent()) {
      context.getStdErr().println("No existing backups found - nothing to verify.");
      return -1;
    }
    CatalogReader catalogReader = CatalogReaders.fromBlockStore(blockStore, catalog.get());

    boolean allFound = true;
    Set<PersistedBlock> allBlocks = blockStore.listAllBlocks();
    Iterator<CatalogReader.CatalogFile> it = catalogReader.walk();
    while (it.hasNext()) {
      CatalogReader.CatalogFile file = it.next();
      for (PersistedBlock block : file.blockList) {
        if (!allBlocks.contains(block)) {
          context.getStdOut().printf("missing block: %s in %s", block, file.path.getHumanReadableString());
          allFound = false;
        }
      }
    }

    if (allFound) {
      context.getStdOut().println("Catalog is complete.");
      return 0;
    } else {
      return -1;
    }
  }
}

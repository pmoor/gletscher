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
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@Command(name = "search", description = "Look for a file name in a remote backup.")
class SearchCommand extends AbstractCommand {
  SearchCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    addSnapshotsOption(
        options, "Search the last N snapshots, or 'all' (default: all).");
    options.addOption(
        Option.builder("a")
            .longOpt("all")
            .desc(
                "Keep searching through all selected snapshots instead of stopping at the first"
                    + " snapshot with matches.")
            .get());
    addMaxListingAgeOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    int snapshotLimit = parseSnapshotsOption(this, commandLine, Integer.MAX_VALUE);
    boolean searchAllSnapshots = commandLine.hasOption("all");

    List<Pattern> positive = new ArrayList<>();
    List<Pattern> negative = new ArrayList<>();
    for (String arg : args) {
      boolean isNegative = false;
      if (arg.startsWith("-")) {
        arg = arg.substring(1);
        isNegative = true;
      }
      try {
        Pattern pattern = Pattern.compile(arg);
        (isNegative ? negative : positive).add(pattern);
      } catch (PatternSyntaxException e) {
        throw new InvalidUsageException(this, "Invalid pattern '" + arg + "': " + e.getMessage());
      }
    }

    boolean found = false;
    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(snapshotLimit);
    for (CloudFileStorage.FileHeader header : headers) {
      CatalogStore.Snapshot snapshot = catalogStore.loadSnapshot(header);
      CatalogReader catalogReader = CatalogReaders.fromBlockStore(blockStore, snapshot.catalog());
      Iterator<CatalogReader.CatalogFile> iterator = catalogReader.walk();
      boolean matchedInSnapshot = false;
      while (iterator.hasNext()) {
        CatalogReader.CatalogFile file = iterator.next();
        String humanReadablePath = file.path.getHumanReadableString();
        if (matches(humanReadablePath, positive, negative)) {
          if (!matchedInSnapshot) {
            String label =
                snapshot.catalogBlock().map(PersistedBlock::toString).orElse(snapshot.pointerName());
            context.getStdOut().printf("Backup %s:\n", label);
            matchedInSnapshot = true;
            found = true;
          }
          context
              .getStdOut()
              .printf(
                  "\t%s: %s (%d bytes)\n",
                  humanReadablePath, file.lastModifiedTime, file.getOriginalSize());
        }
      }
      if (matchedInSnapshot) {
        context.getStdOut().println();
        if (!searchAllSnapshots) {
          break;
        }
      }
    }

    return found ? 0 : -1;
  }

  private static boolean matches(
      String humanReadablePath, List<Pattern> positive, List<Pattern> negative) {
    for (Pattern pattern : positive) {
      if (!pattern.matcher(humanReadablePath).find()) {
        return false;
      }
    }
    for (Pattern pattern : negative) {
      if (pattern.matcher(humanReadablePath).find()) {
        return false;
      }
    }
    return true;
  }
}

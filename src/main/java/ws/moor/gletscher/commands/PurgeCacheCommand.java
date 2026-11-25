/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
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
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CachingCloudFileStorage;
import ws.moor.gletscher.util.ByteSize;

import java.util.List;
import java.util.Set;

/** Purges cached blocks that are not used as metadata in the last N snapshots. */
@Command(
    name = "purge_cache",
    description = "Purge cached blocks not used as metadata in the last N snapshots.")
class PurgeCacheCommand extends AbstractCommand {
  PurgeCacheCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    addSnapshotsOption(
        options, "Keep metadata blocks for the last N snapshots, or 'all' (default: 1).");
    options.addOption(
        Option.builder()
            .longOpt("purge-exists")
            .desc("Also purge cached exists markers for objects not in the kept metadata.")
            .get());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    int snapshotLimit = parseSnapshotsOption(this, commandLine, 1);
    boolean purgeExists = commandLine.hasOption("purge-exists");
    if (cachingCloudFileStorage == null) {
      context.getStdErr().println("Local cache is disabled - nothing to purge.");
      return -1;
    }

    List<CatalogStore.Snapshot> snapshots =
        SnapshotReachability.listSnapshots(catalogStore, snapshotLimit);
    if (snapshots.isEmpty()) {
      context.getStdErr().println("No existing backups found - nothing to purge.");
      return -1;
    }

    boolean followParentLinks = snapshotLimit == Integer.MAX_VALUE;
    Set<String> metadataNames =
        SnapshotReachability.collectReachableObjectNames(
            cloudFileStorage, blockStore, catalogStore, snapshots, false, followParentLinks);
    CachingCloudFileStorage.PurgeStats stats =
        cachingCloudFileStorage.purgeCache(metadataNames, purgeExists);

    context
        .getStdOut()
        .printf(
            "Purged %d cached object(s) (%s) and %d cached listing(s); kept %d metadata object(s) (%s) from %d snapshot(s).%n",
            stats.removedDataCount(),
            ByteSize.ofBytes(stats.removedDataBytes()),
            stats.removedListingsCount(),
            stats.keptDataCount(),
            ByteSize.ofBytes(stats.keptDataBytes()),
            snapshots.size());
    if (purgeExists) {
      context
          .getStdOut()
          .printf(
              "Purged %d cached exists marker(s); kept %d.%n",
              stats.removedExistsCount(),
              stats.keptExistsCount());
    }
    return 0;
  }
}

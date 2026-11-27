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
import ws.moor.gletscher.catalog.CatalogPath;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private record DirKey(CatalogPath path, PersistedBlock block) {}

  private record CachedDirMatches(
      List<CatalogReader.CatalogFile> directMatches, List<DirKey> matchingChildren) {}

  private static final class DirFrame {
    final List<CatalogReader.CatalogFile> directMatches = new ArrayList<>();
    final List<DirKey> matchingChildren = new ArrayList<>();
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
    Set<DirKey> emptyDirs = new HashSet<>();
    Map<DirKey, CachedDirMatches> matchedDirs = new HashMap<>();

    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(snapshotLimit);
    for (CloudFileStorage.FileHeader header : headers) {
      CatalogStore.Snapshot snapshot = catalogStore.loadSnapshot(header);
      String label =
          snapshot.catalogBlock().map(PersistedBlock::toString).orElse(snapshot.pointerName());
      SnapshotMatchPrinter printer = new SnapshotMatchPrinter(label);
      Deque<DirFrame> frameStack = new ArrayDeque<>();

      CatalogReader catalogReader = CatalogReaders.fromBlockStore(blockStore, snapshot.catalog());
      catalogReader.walk(
          new CatalogReader.Visitor() {
            @Override
            public CatalogReader.VisitResult preVisitDirectory(
                CatalogPath path, PersistedBlock block) {
              DirKey key = new DirKey(path, block);
              if (emptyDirs.contains(key)) {
                return CatalogReader.VisitResult.SKIP_SUBTREE;
              }
              if (matchedDirs.containsKey(key)) {
                replayCachedMatches(key, matchedDirs, printer);
                if (!frameStack.isEmpty()) {
                  frameStack.peek().matchingChildren.add(key);
                }
                return CatalogReader.VisitResult.SKIP_SUBTREE;
              }
              frameStack.push(new DirFrame());
              return CatalogReader.VisitResult.CONTINUE;
            }

            @Override
            public CatalogReader.VisitResult visitFile(CatalogReader.CatalogFile file) {
              if (matches(file.path.getHumanReadableString(), positive, negative)) {
                printer.printMatch(file);
                frameStack.peek().directMatches.add(file);
              }
              return CatalogReader.VisitResult.CONTINUE;
            }

            @Override
            public CatalogReader.VisitResult postVisitDirectory(
                CatalogPath path, PersistedBlock block) {
              DirFrame frame = frameStack.pop();
              DirKey key = new DirKey(path, block);
              if (frame.directMatches.isEmpty() && frame.matchingChildren.isEmpty()) {
                emptyDirs.add(key);
              } else {
                matchedDirs.put(
                    key,
                    new CachedDirMatches(
                        List.copyOf(frame.directMatches), List.copyOf(frame.matchingChildren)));
                if (!frameStack.isEmpty()) {
                  frameStack.peek().matchingChildren.add(key);
                }
              }
              return CatalogReader.VisitResult.CONTINUE;
            }
          });

      if (printer.matchedInSnapshot) {
        found = true;
        context.getStdOut().println();
        if (!searchAllSnapshots) {
          break;
        }
      }
    }

    return found ? 0 : -1;
  }

  private final class SnapshotMatchPrinter {
    private final String label;
    private boolean matchedInSnapshot = false;

    private SnapshotMatchPrinter(String label) {
      this.label = label;
    }

    void printMatch(CatalogReader.CatalogFile file) {
      if (!matchedInSnapshot) {
        context.getStdOut().printf("Backup %s:\n", label);
        matchedInSnapshot = true;
      }
      context
          .getStdOut()
          .printf(
              "\t%s: %s (%d bytes)\n",
              file.path.getHumanReadableString(), file.lastModifiedTime, file.getOriginalSize());
    }
  }

  private static void replayCachedMatches(
      DirKey rootKey, Map<DirKey, CachedDirMatches> matchedDirs, SnapshotMatchPrinter printer) {
    Deque<DirKey> stack = new ArrayDeque<>();
    stack.push(rootKey);
    while (!stack.isEmpty()) {
      CachedDirMatches cached = matchedDirs.get(stack.pop());
      for (CatalogReader.CatalogFile file : cached.directMatches()) {
        printer.printMatch(file);
      }
      List<DirKey> children = cached.matchingChildren();
      for (int i = children.size() - 1; i >= 0; i--) {
        stack.push(children.get(i));
      }
    }
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

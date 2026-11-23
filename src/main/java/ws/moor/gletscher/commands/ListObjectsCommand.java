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

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.ByteSize;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Lists remote object names (or gs:// URIs) matching object type, snapshot, size, age, and storage class filters. */
@Command(
    name = "list_objects",
    description = "List remote GCS object names matching metadata/data/orphaned, size, age, and storage class filters.")
class ListObjectsCommand extends AbstractCommand {

  private static final Pattern COMPACT_DURATION_PATTERN =
      Pattern.compile("^([0-9]+)\\s*([dDhHmMsS])$");

  private enum ObjectTypeFilter {
    ALL,
    METADATA,
    DATA,
    ORPHANED;

    static ObjectTypeFilter parse(String raw) {
      return switch (raw.trim().toLowerCase(Locale.ROOT)) {
        case "all" -> ALL;
        case "metadata" -> METADATA;
        case "data" -> DATA;
        case "orphaned" -> ORPHANED;
        default -> throw new IllegalArgumentException(
            "Invalid --type value '" + raw + "'; expected metadata, data, orphaned, or all.");
      };
    }
  }

  private record FilterOptions(
      ObjectTypeFilter type,
      Integer snapshots,
      Long minSizeBytes,
      Long maxSizeBytes,
      Duration minAge,
      Duration maxAge,
      Set<String> includeStorageClasses,
      Set<String> excludeStorageClasses,
      boolean gsUri) {

    boolean hasHeaderFilter() {
      return minSizeBytes != null
          || maxSizeBytes != null
          || minAge != null
          || maxAge != null
          || !includeStorageClasses.isEmpty()
          || !excludeStorageClasses.isEmpty();
    }

    boolean matches(CloudFileStorage.FileHeader header, Instant now) {
      if (minSizeBytes != null && header.size < minSizeBytes) {
        return false;
      }
      if (maxSizeBytes != null && header.size > maxSizeBytes) {
        return false;
      }
      if (minAge != null || maxAge != null) {
        if (header.createdTime == null) {
          return false;
        }
        Duration age = Duration.between(header.createdTime, now);
        if (minAge != null && age.compareTo(minAge) < 0) {
          return false;
        }
        if (maxAge != null && age.compareTo(maxAge) > 0) {
          return false;
        }
      }
      if (!includeStorageClasses.isEmpty()) {
        if (header.storageClass == null
            || !includeStorageClasses.contains(header.storageClass.toUpperCase(Locale.ROOT))) {
          return false;
        }
      }
      if (!excludeStorageClasses.isEmpty() && header.storageClass != null) {
        if (excludeStorageClasses.contains(header.storageClass.toUpperCase(Locale.ROOT))) {
          return false;
        }
      }
      return true;
    }
  }

  ListObjectsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    options.addOption(
        Option.builder("t")
            .longOpt("type")
            .hasArg()
            .argName("type")
            .desc("Object type to list: metadata, data, orphaned, or all (default: all).")
            .get());
    addSnapshotsOption(
        options,
        "For metadata or orphaned, scope reachability to the last N snapshots (or 'all').");
    options.addOption(
        Option.builder()
            .longOpt("min-size")
            .hasArg()
            .argName("size")
            .desc("Minimum object size (e.g. 1024, 10M, 1GiB).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("max-size")
            .hasArg()
            .argName("size")
            .desc("Maximum object size (e.g. 1024, 10M, 1GiB).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("min-age")
            .hasArg()
            .argName("duration")
            .desc("Minimum object age based on creation time (e.g. 90d, 24h, P30D).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("max-age")
            .hasArg()
            .argName("duration")
            .desc("Maximum object age based on creation time (e.g. 90d, 24h, P30D).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("storage-class")
            .hasArg()
            .argName("class")
            .desc("Include only objects with this GCS storage class (repeatable or comma-separated).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("exclude-storage-class")
            .hasArg()
            .argName("class")
            .desc("Exclude objects with this GCS storage class (repeatable or comma-separated).")
            .get());
    options.addOption(
        Option.builder()
            .longOpt("gs-uri")
            .desc("Output full gs://<bucket>/<prefix><name> URIs instead of relative names.")
            .get());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept positional arguments.");
    }

    FilterOptions filters = parseOptions(commandLine);
    String uriPrefix = "";
    if (filters.gsUri()) {
      String bucket = config.getBucketName();
      if (Strings.isNullOrEmpty(bucket)) {
        throw new IllegalStateException("bucket_name is required in configuration for --gs-uri");
      }
      uriPrefix = "gs://" + bucket + "/" + Strings.nullToEmpty(config.getFilePrefix());
    }

    Instant now = context.getClock().instant();
    ListingResult result = findMatchingObjects(filters, now);
    for (String name : result.names()) {
      context.getStdOut().println(uriPrefix + name);
    }
    printSummary(result, context.getStdErr());
    return 0;
  }

  private record ListingResult(
      List<String> names, Optional<List<CloudFileStorage.FileHeader>> headers) {}

  private static final class ClassSummary {
    long count = 0;
    long bytes = 0;

    void add(long size) {
      count++;
      bytes += size;
    }
  }

  private void printSummary(ListingResult result, PrintStream err) {
    if (result.names().isEmpty()) {
      return;
    }
    if (result.headers().isEmpty()) {
      int count = result.names().size();
      err.printf(
          "%d %s (sizes and storage classes omitted; pass --min-size 0 to include)%n",
          count,
          count == 1 ? "object" : "objects");
      return;
    }

    Map<String, ClassSummary> byClass = new TreeMap<>();
    long totalCount = 0;
    long totalBytes = 0;
    for (CloudFileStorage.FileHeader header : result.headers().get()) {
      String storageClass = header.storageClass != null ? header.storageClass : "UNKNOWN";
      byClass.computeIfAbsent(storageClass, ignored -> new ClassSummary()).add(header.size);
      totalCount++;
      totalBytes += header.size;
    }

    for (Map.Entry<String, ClassSummary> entry : byClass.entrySet()) {
      ClassSummary summary = entry.getValue();
      err.printf(
          "%s: %d %s, %s%n",
          entry.getKey(),
          summary.count,
          summary.count == 1 ? "object" : "objects",
          ByteSize.ofBytes(summary.bytes));
    }
    if (byClass.size() > 1) {
      err.printf(
          "Total: %d %s, %s%n",
          totalCount,
          totalCount == 1 ? "object" : "objects",
          ByteSize.ofBytes(totalBytes));
    }
  }

  private ListingResult findMatchingObjects(FilterOptions filters, Instant now) {
    if (filters.type() == ObjectTypeFilter.METADATA) {
      int limit = filters.snapshots() != null ? filters.snapshots() : Integer.MAX_VALUE;
      boolean followBaseCatalogs = limit == Integer.MAX_VALUE;
      List<CatalogStore.Snapshot> snapshots = listSnapshots(limit);
      Set<String> metadataNames =
          collectReachableObjectNames(snapshots, false, followBaseCatalogs);
      if (!filters.hasHeaderFilter()) {
        return new ListingResult(new ArrayList<>(new TreeSet<>(metadataNames)), Optional.empty());
      }
      if (metadataNames.isEmpty()) {
        return new ListingResult(List.of(), Optional.of(List.of()));
      }
      return filterHeaders(listAllHeaders(), header -> metadataNames.contains(header.name), filters, now);
    }

    if (filters.type() == ObjectTypeFilter.ORPHANED) {
      int limit = filters.snapshots() != null ? filters.snapshots() : Integer.MAX_VALUE;
      boolean followBaseCatalogs = limit == Integer.MAX_VALUE;
      List<CatalogStore.Snapshot> snapshots = listSnapshots(limit);
      Set<String> reachableNames =
          collectReachableObjectNames(snapshots, true, followBaseCatalogs);
      return filterHeaders(
          listAllHeaders(),
          header -> !reachableNames.contains(header.name),
          filters,
          now);
    }

    if (filters.type() == ObjectTypeFilter.DATA) {
      List<CatalogStore.Snapshot> snapshots = listSnapshots(Integer.MAX_VALUE);
      Set<String> metadataNames = collectReachableObjectNames(snapshots, false, true);
      return filterHeaders(
          blockStore.listAllBlockHeaders(),
          header -> !metadataNames.contains(header.name),
          filters,
          now);
    }

    return filterHeaders(listAllHeaders(), header -> true, filters, now);
  }

  private List<CatalogStore.Snapshot> listSnapshots(int limit) {
    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(limit);
    List<CatalogStore.Snapshot> snapshots = new ArrayList<>(headers.size());
    for (CloudFileStorage.FileHeader header : headers) {
      snapshots.add(catalogStore.loadSnapshot(header));
    }
    return snapshots;
  }

  private Set<String> collectReachableObjectNames(
      List<CatalogStore.Snapshot> snapshots,
      boolean includeDataBlocks,
      boolean followBaseCatalogs) {
    Set<String> names = new LinkedHashSet<>();
    Set<PersistedBlock> visitedCatalogs = new HashSet<>();
    Set<PersistedBlock> visitedDirectories = new HashSet<>();
    Deque<Catalog> catalogQueue = new ArrayDeque<>();
    Deque<PersistedBlock> dirQueue = new ArrayDeque<>();

    for (CatalogStore.Snapshot snapshot : snapshots) {
      names.add(snapshot.pointerName());
      snapshot
          .catalogBlock()
          .ifPresent(
              pb -> {
                names.add(BlockStore.toFileName(pb));
                visitedCatalogs.add(pb);
              });
      catalogQueue.push(snapshot.catalog());
    }

    while (!catalogQueue.isEmpty()) {
      Catalog catalog = catalogQueue.pop();
      for (PersistedBlock rootBlock : catalog.getRoots().values()) {
        if (visitedDirectories.add(rootBlock)) {
          dirQueue.push(rootBlock);
        }
      }
      if (followBaseCatalogs) {
        catalog
            .getBaseCatalog()
            .ifPresent(
                baseBlock -> {
                  String fileName = BlockStore.toFileName(baseBlock);
                  if (visitedCatalogs.add(baseBlock)
                      && Boolean.TRUE.equals(
                          Futures.getUnchecked(cloudFileStorage.exists(fileName)))) {
                    names.add(fileName);
                    catalogQueue.push(catalogStore.load(baseBlock));
                  }
                });
      }
    }

    while (!dirQueue.isEmpty()) {
      PersistedBlock dirBlock = dirQueue.pop();
      names.add(BlockStore.toFileName(dirBlock));
      byte[] bytes = Futures.getUnchecked(blockStore.retrieve(dirBlock));
      try {
        Gletscher.Directory dir = Gletscher.Directory.parseFrom(bytes);
        for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
          if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.DIRECTORY) {
            PersistedBlock childBlock = PersistedBlock.fromProto(entry.getDirectory().getBlock());
            if (visitedDirectories.add(childBlock)) {
              dirQueue.push(childBlock);
            }
          } else if (includeDataBlocks
              && entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
            for (Gletscher.PersistedBlock fileBlock : entry.getFile().getBlockList()) {
              names.add(BlockStore.toFileName(PersistedBlock.fromProto(fileBlock)));
            }
          }
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Failed to parse directory block: " + dirBlock, e);
      }
    }

    return names;
  }

  private List<CloudFileStorage.FileHeader> listAllHeaders() {
    List<CloudFileStorage.FileHeader> all = new ArrayList<>();
    Iterator<CloudFileStorage.FileHeader> backups =
        cloudFileStorage.listFiles("backups/", Integer.MAX_VALUE);
    while (backups.hasNext()) {
      all.add(backups.next());
    }
    all.addAll(blockStore.listAllBlockHeaders());
    return all;
  }

  private static ListingResult filterHeaders(
      List<CloudFileStorage.FileHeader> headers,
      java.util.function.Predicate<CloudFileStorage.FileHeader> membershipFilter,
      FilterOptions filters,
      Instant now) {
    List<CloudFileStorage.FileHeader> matched = new ArrayList<>();
    for (CloudFileStorage.FileHeader header : headers) {
      if (membershipFilter.test(header) && filters.matches(header, now)) {
        matched.add(header);
      }
    }
    matched.sort(Comparator.comparing(h -> h.name));
    List<String> names = new ArrayList<>(matched.size());
    for (CloudFileStorage.FileHeader header : matched) {
      names.add(header.name);
    }
    return new ListingResult(names, Optional.of(matched));
  }

  private FilterOptions parseOptions(CommandLine commandLine) throws InvalidUsageException {
    ObjectTypeFilter type = ObjectTypeFilter.ALL;
    if (commandLine.hasOption("type")) {
      try {
        type = ObjectTypeFilter.parse(commandLine.getOptionValue("type"));
      } catch (IllegalArgumentException e) {
        throw new InvalidUsageException(this, e.getMessage());
      }
    }

    Integer snapshots = null;
    if (commandLine.hasOption("snapshots")) {
      if (type != ObjectTypeFilter.METADATA && type != ObjectTypeFilter.ORPHANED) {
        throw new InvalidUsageException(
            this, "--snapshots can only be used with --type metadata or --type orphaned.");
      }
      snapshots = parseSnapshotsOption(this, commandLine, Integer.MAX_VALUE);
    }

    Long minSizeBytes = parseSizeOption(commandLine, "min-size");
    Long maxSizeBytes = parseSizeOption(commandLine, "max-size");
    Duration minAge = parseDurationOption(commandLine, "min-age");
    Duration maxAge = parseDurationOption(commandLine, "max-age");
    Set<String> includeStorageClasses = parseStorageClassOptions(commandLine, "storage-class");
    Set<String> excludeStorageClasses =
        parseStorageClassOptions(commandLine, "exclude-storage-class");

    return new FilterOptions(
        type,
        snapshots,
        minSizeBytes,
        maxSizeBytes,
        minAge,
        maxAge,
        includeStorageClasses,
        excludeStorageClasses,
        commandLine.hasOption("gs-uri"));
  }

  private Long parseSizeOption(CommandLine commandLine, String option)
      throws InvalidUsageException {
    if (!commandLine.hasOption(option)) {
      return null;
    }
    String raw = commandLine.getOptionValue(option);
    try {
      return ByteSize.parse(raw).toBytes();
    } catch (IllegalArgumentException e) {
      throw new InvalidUsageException(this, "Invalid --" + option + ": " + e.getMessage());
    }
  }

  private Duration parseDurationOption(CommandLine commandLine, String option)
      throws InvalidUsageException {
    if (!commandLine.hasOption(option)) {
      return null;
    }
    String raw = commandLine.getOptionValue(option);
    try {
      Duration duration = parseDuration(raw);
      if (duration.isNegative()) {
        throw new InvalidUsageException(this, "--" + option + " must be non-negative: " + raw);
      }
      return duration;
    } catch (IllegalArgumentException | DateTimeParseException e) {
      throw new InvalidUsageException(this, "Invalid --" + option + " duration: " + raw);
    }
  }

  private static Duration parseDuration(String input) {
    String trimmed = input.trim();
    Matcher matcher = COMPACT_DURATION_PATTERN.matcher(trimmed);
    if (matcher.matches()) {
      long amount = Long.parseLong(matcher.group(1));
      return switch (matcher.group(2).toLowerCase(Locale.ROOT)) {
        case "d" -> Duration.ofDays(amount);
        case "h" -> Duration.ofHours(amount);
        case "m" -> Duration.ofMinutes(amount);
        case "s" -> Duration.ofSeconds(amount);
        default -> throw new IllegalArgumentException("Unsupported duration unit: " + input);
      };
    }
    return Duration.parse(trimmed.toUpperCase(Locale.ROOT));
  }

  private Set<String> parseStorageClassOptions(CommandLine commandLine, String option)
      throws InvalidUsageException {
    String[] values = commandLine.getOptionValues(option);
    if (values == null) {
      return Set.of();
    }
    Set<String> classes = new HashSet<>();
    for (String value : values) {
      for (String part : value.split(",")) {
        String normalized = part.trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
          throw new InvalidUsageException(this, "Empty storage class in --" + option);
        }
        classes.add(normalized);
      }
    }
    return classes;
  }
}

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CachingCloudFileStorage;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CompressingCloudFileStorage;
import ws.moor.gletscher.cloud.CostTracker;
import ws.moor.gletscher.cloud.CountingCloudFileStorage;
import ws.moor.gletscher.cloud.EncryptingCloudFileStorage;
import ws.moor.gletscher.cloud.SigningCloudFileStorage;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class AbstractCommand {

  private static final Pattern COMPACT_DURATION_PATTERN =
      Pattern.compile("^([0-9]+)\\s*([dDhHmMsS])$");

  final CommandContext context;
  final AtomicBoolean hasRun = new AtomicBoolean(false);

  private boolean hasConfigArg = false;
  private boolean hasMaxListingAgeArg = false;
  private final CostTracker costTracker = new CostTracker();

  protected Configuration config;
  protected CloudFileStorage cloudFileStorage;
  protected BlockStore blockStore;
  protected CatalogStore catalogStore;

  AbstractCommand(CommandContext context) {
    this.context = context;
  }

  final int run(String[] args) throws Exception {
    Preconditions.checkState(!hasRun.getAndSet(true), "Command already ran");

    Options options = new Options();
    addCommandLineOptions(options);

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      throw new InvalidUsageException(this, e.getMessage());
    }

    if (hasConfigArg) {
      config = loadConfig(commandLine);
      Duration maxListingAge = Duration.ZERO;
      if (hasMaxListingAgeArg) {
        Duration parsed = parseDurationOption(this, commandLine, "max-listing-age");
        if (parsed != null) {
          maxListingAge = parsed;
        }
      }
      if (config.disableCache() && !maxListingAge.isZero()) {
        context
            .getStdErr()
            .println("Warning: --max-listing-age ignored because disable_cache is true.");
      }
      cloudFileStorage = buildCloudFileStorage(config, costTracker, maxListingAge);
      blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
      catalogStore = new CatalogStore(context.getFileSystem(), cloudFileStorage, blockStore);
    }

    List<String> argList = new ArrayList<>(commandLine.getArgList());
    int returnCode = runInternal(commandLine, argList);

    if (cloudFileStorage != null) {
      cloudFileStorage.close(); // caches can clean-up
    }

    if (costTracker.hasUsage()) {
      costTracker.printSummary(context.getStdErr());
    }
    return returnCode;
  }

  void addCommandLineOptions(Options options) {
  }

  protected abstract int runInternal(CommandLine commandLine, List<String> args) throws Exception;

  final void addConfigFileOption(Options options) {
    hasConfigArg = true;
    options.addOption(
        Option.builder("c")
            .longOpt("config")
            .required()
            .hasArg()
            .argName("FILE")
            .desc("Path to the configuration file.")
            .get());
  }

  final void addMaxListingAgeOption(Options options) {
    hasMaxListingAgeArg = true;
    options.addOption(
        Option.builder()
            .longOpt("max-listing-age")
            .hasArg()
            .argName("duration")
            .desc("Allow using cached remote file listings up to this age (e.g. 1h, 24h, 7d).")
            .get());
  }

  protected static Duration parseDurationOption(
      AbstractCommand command, CommandLine commandLine, String option)
      throws InvalidUsageException {
    if (!commandLine.hasOption(option)) {
      return null;
    }
    String raw = commandLine.getOptionValue(option);
    try {
      Duration duration = parseDuration(raw);
      if (duration.isNegative()) {
        throw new InvalidUsageException(command, "--" + option + " must be non-negative: " + raw);
      }
      return duration;
    } catch (IllegalArgumentException | DateTimeParseException e) {
      throw new InvalidUsageException(command, "Invalid --" + option + " duration: " + raw);
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

  protected static void addSnapshotsOption(Options options, String description) {
    options.addOption(
        Option.builder("n")
            .longOpt("snapshots")
            .hasArg()
            .argName("count|all")
            .desc(description)
            .get());
  }

  protected static int parseSnapshotsOption(
      AbstractCommand command, CommandLine commandLine, int defaultValue)
      throws InvalidUsageException {
    if (!commandLine.hasOption("snapshots")) {
      return defaultValue;
    }
    String rawSnapshots = commandLine.getOptionValue("snapshots").trim();
    if ("all".equalsIgnoreCase(rawSnapshots)) {
      return Integer.MAX_VALUE;
    }
    int snapshots;
    try {
      snapshots = Integer.parseInt(rawSnapshots);
    } catch (NumberFormatException e) {
      throw new InvalidUsageException(command, "Invalid --snapshots value: " + rawSnapshots);
    }
    if (snapshots <= 0) {
      throw new InvalidUsageException(
          command, "--snapshots must be at least 1 or 'all': " + snapshots);
    }
    return snapshots;
  }

  private CloudFileStorage buildCloudFileStorage(
      Configuration config, CostTracker costTracker, Duration maxListingAge) {
    CloudFileStorage cloudFileStorage = context.connectToCloud(config, costTracker);

    CountingCloudFileStorage counting = new CountingCloudFileStorage(cloudFileStorage);
    cloudFileStorage = counting;
    if (!config.disableCache()) {
      cloudFileStorage =
          new CachingCloudFileStorage(
              counting, config.getLocalCacheDir(), maxListingAge, context.getClock());
    }
    cloudFileStorage =
        new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage =
        new EncryptingCloudFileStorage(
            cloudFileStorage, new Cryptor(config.getEncryptionKey(), config.getSigningKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());
    return cloudFileStorage;
  }

  void printUsage(PrintStream out) {
    Options options = new Options();
    addCommandLineOptions(options);
    Collection<Option> registeredOptions = options.getOptions();

    out.println("Usage:");
    out.println(
        "  gletscher " + getCommandName() + (registeredOptions.isEmpty() ? "" : " [options]"));
    out.println();
    out.println(getCommandDescription());

    if (!registeredOptions.isEmpty()) {
      out.println();
      out.println("Options:");
      printOptions(out, registeredOptions);
    }
  }

  private static void printOptions(PrintStream out, Collection<Option> options) {
    boolean hasAnyShortOpt = options.stream().anyMatch(option -> option.getOpt() != null);
    List<String> formattedFlags = new ArrayList<>(options.size());
    int longestFlagLength = 0;
    for (Option option : options) {
      String formatted = formatOptionFlags(option, hasAnyShortOpt);
      formattedFlags.add(formatted);
      longestFlagLength = Math.max(longestFlagLength, formatted.length());
    }

    int index = 0;
    for (Option option : options) {
      String formatted = formattedFlags.get(index++);
      String description = Strings.nullToEmpty(option.getDescription());
      if (option.isRequired()) {
        description = description.isEmpty() ? "(required)" : description + " (required)";
      }
      if (description.isEmpty()) {
        out.println("  " + formatted);
      } else {
        out.printf("  %-" + longestFlagLength + "s  %s%n", formatted, description);
      }
    }
  }

  private static String formatOptionFlags(Option option, boolean hasAnyShortOpt) {
    StringBuilder sb = new StringBuilder();
    if (option.getOpt() != null) {
      sb.append('-').append(option.getOpt());
      if (option.hasLongOpt()) {
        sb.append(", --").append(option.getLongOpt());
      }
    } else {
      if (hasAnyShortOpt) {
        sb.append("    ");
      }
      if (option.hasLongOpt()) {
        sb.append("--").append(option.getLongOpt());
      }
    }
    if (option.hasArg()) {
      String argName = option.hasArgName() ? option.getArgName() : "arg";
      if (option.hasOptionalArg()) {
        sb.append(" [<").append(argName).append(">]");
      } else {
        sb.append(" <").append(argName).append('>');
      }
    }
    return sb.toString();
  }

  private Configuration loadConfig(CommandLine commandLine) throws IOException {
    return Configuration.fromFile(
        context.getFileSystem().getPath(commandLine.getOptionValue("config")));
  }

  private String getCommandName() {
    Command annotation = this.getClass().getAnnotation(Command.class);
    return annotation.name();
  }

  private String getCommandDescription() {
    Command annotation = this.getClass().getAnnotation(Command.class);
    return annotation.description();
  }
}

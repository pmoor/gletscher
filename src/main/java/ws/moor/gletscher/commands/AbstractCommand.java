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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractCommand {

  final CommandContext context;
  final AtomicBoolean hasRun = new AtomicBoolean(false);

  private boolean hasConfigArg = false;
  private final CostTracker costTracker = new CostTracker();

  protected Configuration config;
  private CloudFileStorage cloudFileStorage;
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
      cloudFileStorage = buildCloudFileStorage(config, costTracker);
      blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
      catalogStore = new CatalogStore(context.getFileSystem(), cloudFileStorage);
    }

    List<String> argList = new ArrayList<>(commandLine.getArgList());
    int returnCode = runInternal(commandLine, argList);

    if (cloudFileStorage != null) {
      cloudFileStorage.close();  // caches can clean-up
    }

    if (costTracker.hasUsage()) {
      costTracker.printSummary(context.getStdErr());
    }
    return returnCode;
  }

  void addCommandLineOptions(Options options) {}

  protected abstract int runInternal(CommandLine commandLine, List<String> args) throws Exception;

  final void addConfigFileOption(Options options) {
    hasConfigArg = true;
    options.addOption(Option.builder("c").longOpt("config").required().hasArg().argName("FILE").build());
  }

  private CloudFileStorage buildCloudFileStorage(Configuration config, CostTracker costTracker) {
    CloudFileStorage cloudFileStorage = context.connectToCloud(config, costTracker);

    CountingCloudFileStorage counting = new CountingCloudFileStorage(cloudFileStorage);
    cloudFileStorage = counting;
    if (!config.disableCache()) {
      cloudFileStorage = new CachingCloudFileStorage(counting, config.getLocalCacheDir(), context.getClock());
    }
    cloudFileStorage = new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage = new EncryptingCloudFileStorage(
        cloudFileStorage, new Cryptor(config.getEncryptionKey(), config.getSigningKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());
    return cloudFileStorage;
  }

  void printUsage(PrintStream out) {
    // TODO(pmoor): customize further
    out.println("Usage:");
    out.println("  gletscher " + getCommandName());
    out.println();
    out.println(getCommandDescription());
  }

  private Configuration loadConfig(CommandLine commandLine) throws IOException {
    return Configuration.fromFile(context.getFileSystem().getPath(commandLine.getOptionValue("config")));
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

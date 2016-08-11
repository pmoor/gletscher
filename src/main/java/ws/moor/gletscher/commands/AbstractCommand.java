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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.cloud.CachingCloudFileStorage;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CompressingCloudFileStorage;
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

abstract class AbstractCommand {

  protected final CommandContext context;

  protected AbstractCommand(CommandContext context) {
    this.context = context;
  }

  final int run(String[] args) throws Exception {
    Options options = new Options();
    addCommandLineOptions(options);

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      throw new InvalidUsageException(this, e.getMessage());
    }

    List<String> argList = new ArrayList<>(commandLine.getArgList());
    return runInternal(commandLine, argList);
  }

  protected void addCommandLineOptions(Options options) {}

  protected abstract int runInternal(CommandLine commandLine, List<String> args) throws Exception;

  protected final void addConfigFileOption(Options options) {
    options.addOption(Option.builder("c").longOpt("config").required().hasArg().argName("FILE").build());
  }

  protected CloudFileStorage buildCloudFileStorage(Configuration config) {
    CloudFileStorage cloudFileStorage = context.connectToCloud(config);

    CountingCloudFileStorage counting = new CountingCloudFileStorage(cloudFileStorage);
    cloudFileStorage = counting;
    if (!config.disableCache()) {
      cloudFileStorage = new CachingCloudFileStorage(counting, config.getLocalCacheDir(), context.getExecutor());
    }
    cloudFileStorage = new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage = new EncryptingCloudFileStorage(cloudFileStorage, new Cryptor(config.getEncryptionKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());
    return cloudFileStorage;
  }

  void printUsage(PrintStream out) {
    // TODO(pmoor): customize further
    out.println("Usage: gletscher " + getCommandName());
    out.println();
    out.println(getCommandDescription());
  }

  protected Configuration loadConfig(CommandLine commandLine) throws IOException {
    return Configuration.fromFile(context.getFileSystem().getPath(commandLine.getOptionValue("config")));
  }

  String getCommandName() {
    Command annotation = this.getClass().getAnnotation(Command.class);
    return annotation.name();
  }

  String getCommandDescription() {
    Command annotation = this.getClass().getAnnotation(Command.class);
    return annotation.description();
  }
}

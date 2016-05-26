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

package ws.moor.gletscher;

import com.google.api.services.storage.Storage;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import org.apache.commons.cli.*;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.*;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class GletscherMain {

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(Option.builder("c").longOpt("config").required().hasArg().argName("FILE").build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    List<String> argList = commandLine.getArgList();
    if (argList.isEmpty() || !argList.get(0).equals("backup")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("gletscher -c config.properties backup <dir> [<dir> [<dir> ...]]", options);
      return;
    }

    FileSystem fs = FileSystems.getDefault();
    ImmutableSet.Builder<Path> backupPathsBuilder = ImmutableSet.builder();
    for (int i = 1; i < argList.size(); i++) {
      Path path = fs.getPath(argList.get(i));
      if (!Files.isDirectory(path)) {
        System.err.printf("not a directory: %s\n", argList.get(i));
        return;
      }
      backupPathsBuilder.add(path);
    }

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("gletscher-pool-%d")
        .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
        .build();
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        new ThreadPoolExecutor(4, 32, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1024), threadFactory));

    Configuration config = new Configuration(fs.getPath(commandLine.getOptionValue("config")));
    Storage storage = GoogleCloudFileStorage.buildStorageWithCredentials(config.getCredentialFilePath());
    GoogleCloudFileStorage gcfs = new GoogleCloudFileStorage(storage, config.getBucketName(), config.getFilePrefix(), executor);
    CountingCloudFileStorage counting = new CountingCloudFileStorage(gcfs);
    CloudFileStorage cloudFileStorage = new CachingCloudFileStorage(counting, config.getLocalCacheDir(), executor);
    cloudFileStorage = new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage = new EncryptingCloudFileStorage(cloudFileStorage, new Cryptor(config.getEncryptionKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());

    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(cloudFileStorage, Clock.systemUTC());

    Gletscher.BackupRecord lastBackup = catalogStore.findLatestBackup();
    PersistedBlock lastBackupRoot = PersistedBlock.fromProto(lastBackup.getRootDirectory());

    StreamSplitter splitter = config.getStreamSplitter();
    CatalogReader catalogReader = new CatalogReader(blockStore, lastBackupRoot);
    FileSystemReader<PersistedBlock> fileSystemReader = new FileSystemReader<>(
        backupPathsBuilder.build(), config.getSkippedPaths());

    BackUpper backUpper = new BackUpper(catalogReader, splitter, blockStore);
    PersistedBlock newRoot = fileSystemReader.start(backUpper);

    catalogStore.store(newRoot);
    System.out.println("new root: " + newRoot);
    System.out.println(counting);

    executor.shutdown();
  }

}

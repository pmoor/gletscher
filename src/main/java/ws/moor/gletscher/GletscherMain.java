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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.*;
import org.apache.commons.cli.*;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.catalog.CompositeCatalogReader;
import ws.moor.gletscher.catalog.RootCatalogReader;
import ws.moor.gletscher.cloud.*;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class GletscherMain {

  private final FileSystem fileSystem;
  private final InputStream stdin;
  private final PrintStream stdout;
  private final PrintStream stderr;

  private CloudFileStorage cloudFileStorage = null;

  GletscherMain(FileSystem fileSystem, InputStream in, PrintStream out, PrintStream err) {
    this.fileSystem = fileSystem;
    this.stdin = in;
    this.stdout = out;
    this.stderr = err;
  }

  public static void main(String[] args) throws Exception {
    GletscherMain main = new GletscherMain(FileSystems.getDefault(), System.in, System.out, System.err);
    System.exit(main.run(args));
  }

  int run(String... args) throws Exception {
    Options options = new Options();
    options.addOption(Option.builder("c").longOpt("config").required().hasArg().argName("FILE").build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    List<String> argList = new ArrayList<>(commandLine.getArgList());
    if (argList.isEmpty()) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("gletscher -c config.properties backup <dir> [<dir> [<dir> ...]]", options);
      return -1;
    }

    Configuration config = Configuration.fromFile(fileSystem.getPath(commandLine.getOptionValue("config")));
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("gletscher-pool-%d")
        .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
        .build();
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        new ThreadPoolExecutor(4, 32, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1024), threadFactory));
    CloudFileStorage cloudFileStorage = this.cloudFileStorage;
    if (cloudFileStorage == null) {
      Storage storage = GoogleCloudFileStorage.buildStorageWithCredentials(config.getCredentialFilePath());
      cloudFileStorage = new GoogleCloudFileStorage(storage, config.getBucketName(), config.getFilePrefix(), executor);
    }
    CountingCloudFileStorage counting = new CountingCloudFileStorage(cloudFileStorage);
    cloudFileStorage = counting;
    if (!config.disableCache()) {
      cloudFileStorage = new CachingCloudFileStorage(counting, config.getLocalCacheDir(), executor);
    }
    cloudFileStorage = new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage = new EncryptingCloudFileStorage(cloudFileStorage, new Cryptor(config.getEncryptionKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());

    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(cloudFileStorage, Clock.systemUTC());

    int response = -1;
    String command = argList.remove(0);
    switch (command) {
      case "backup":
        response = backup(config, blockStore, catalogStore, argList);
        break;
      case "restore":
        response = restore(config, blockStore, catalogStore, argList);
        break;
      default:
        stderr.printf("unknown command: %s\n", command);
        break;
    }

    executor.shutdown();
    stdout.print(counting.toString());
    return response;
  }

  private int backup(Configuration config, BlockStore blockStore, CatalogStore catalogStore, List<String> files) throws IOException {
    ImmutableSet.Builder<Path> backupPathsBuilder = ImmutableSet.builder();
    for (String file : files) {
      Path path = fileSystem.getPath(file);
      if (!Files.isDirectory(path)) {
        stderr.printf("not a directory: %s\n", file);
        return -1;
      }
      backupPathsBuilder.add(path);
    }

    StreamSplitter splitter = config.getStreamSplitter();
    List<CatalogReader> readers = new ArrayList<>();
    for (Gletscher.BackupRecord backupRecord : catalogStore.findLastBackups(10)) {
      PersistedBlock root = PersistedBlock.fromProto(backupRecord.getRootDirectory());
      readers.add(new RootCatalogReader(blockStore, root));
    }
    CatalogReader catalogReader = new CompositeCatalogReader(readers);
    FileSystemReader<PersistedBlock> fileSystemReader = new FileSystemReader<>(
        backupPathsBuilder.build(), config.getSkippedPaths(), stderr);

    BackUpper backUpper = new BackUpper(catalogReader, splitter, blockStore, stdout, stderr);
    PersistedBlock newRoot = fileSystemReader.start(backUpper);

    catalogStore.store(newRoot);
    stdout.println("new root: " + newRoot);
    return 0;
  }

  private int restore(Configuration config, BlockStore blockStore, CatalogStore catalogStore, List<String> files) throws IOException {
    if (files.size() != 1) {
      stderr.println("need exactly one restore root");
      return -1;
    }

    Path restoreRoot = fileSystem.getPath(files.get(0));
    Preconditions.checkState(!Files.exists(restoreRoot, LinkOption.NOFOLLOW_LINKS));
    Files.createDirectories(restoreRoot);

    List<Gletscher.BackupRecord> lastBackups = catalogStore.findLastBackups(1);
    if (lastBackups.isEmpty()) {
      stderr.println("no existing backup found");
      return -1;
    }

    PersistedBlock root = PersistedBlock.fromProto(lastBackups.get(0).getRootDirectory());
    Gletscher.Directory rootDir = Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(root)));
    restoreInner(blockStore, rootDir, restoreRoot);
    return 0;
  }

  private void restoreInner(BlockStore blockStore, Gletscher.Directory dir, Path path) throws IOException {
    for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
      switch (entry.getTypeCase()) {
        case FILE:
          Path actualFile = path.resolve(entry.getFile().getName());
          OutputStream fos = new BufferedOutputStream(Files.newOutputStream(actualFile, StandardOpenOption.CREATE_NEW));
          for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
            byte[] data = Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(block)));
            fos.write(data);
          }
          fos.close();
          Files.setLastModifiedTime(actualFile, FileTime.fromMillis(entry.getFile().getLastModifiedMillis()));
          break;
        case DIRECTORY:
          Path childPath = path.resolve(entry.getDirectory().getName());
          Gletscher.Directory childDir = Gletscher.Directory.parseFrom(
              Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(entry.getDirectory().getBlock()))));
          Files.createDirectory(childPath);
          restoreInner(blockStore, childDir, childPath);
          break;
        case SYMLINK:
          Files.createSymbolicLink(path.resolve(entry.getSymlink().getName()),
              path.getFileSystem().getPath(entry.getSymlink().getTarget()));
          break;
        default:
          throw new IllegalArgumentException(entry.toString());
      }
    }
  }

  @VisibleForTesting
  public void setCloudFileStorageForTesting(CloudFileStorage cloudFileStorage) {
    Preconditions.checkState(this.cloudFileStorage == null);
    this.cloudFileStorage = Preconditions.checkNotNull(cloudFileStorage);
  }
}

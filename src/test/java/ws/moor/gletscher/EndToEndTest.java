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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.blocks.Signature;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.*;
import ws.moor.gletscher.files.FileSystemReader;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class EndToEndTest {

  @Test
  public void testSomething() throws IOException {
    FileSystem fs = Jimfs.newFileSystem();

    Files.write(fs.getPath("/config.properties"),
        ("cache_dir /tmp/cache\n" +
            "secret_key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n").getBytes(StandardCharsets.UTF_8));
    createRandomFileTree(fs.getPath("/home/pmoor"), 4);

    Configuration config = new Configuration(fs.getPath("/config.properties"));
    Files.createDirectories(config.getLocalCacheDir());

    ListeningExecutorService executor = MoreExecutors.newDirectExecutorService();
    InMemoryCloudFileStorage bottomStorage = new InMemoryCloudFileStorage(executor);
    CountingCloudFileStorage countingStorage = new CountingCloudFileStorage(bottomStorage);
    CloudFileStorage cloudFileStorage = new CachingCloudFileStorage(countingStorage, config.getLocalCacheDir(), executor);
    cloudFileStorage = new SigningCloudFileStorage(cloudFileStorage, new Signer(config.getSigningKey()));
    cloudFileStorage = new EncryptingCloudFileStorage(cloudFileStorage, new Cryptor(config.getEncryptionKey()));
    cloudFileStorage = new CompressingCloudFileStorage(cloudFileStorage, new Compressor());

    BlockStore blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
    StreamSplitter splitter = config.getStreamSplitter();
    CatalogStore catalogStore = new CatalogStore(cloudFileStorage, Clock.systemUTC());
    Gletscher.BackupRecord lastBackup = catalogStore.findLatestBackup();
    assertThat(lastBackup).isNull();
    // TODO(pmoor): implement null catalog reader
    PersistedBlock lastBackupRoot = new PersistedBlock(Signature.randomForTest(), 10);
    CatalogReader catalogReader = new CatalogReader(blockStore, lastBackupRoot);
    FileSystemReader<PersistedBlock> fileSystemReader = new FileSystemReader<>(
        ImmutableSet.of(fs.getPath("/home/pmoor")), config.getSkippedPaths());

    BackUpper backUpper = new BackUpper(catalogReader, splitter, blockStore);
    PersistedBlock newRoot = fileSystemReader.start(backUpper);
    System.out.println("new root: " + newRoot);
    System.out.println(countingStorage);
    catalogStore.store(newRoot);

    // second run
    createRandomFileTree(fs.getPath("/", "home", "pmoor", "new child"), 2);
    createRandomFileTree(fs.getPath("/", "home", "cmoor"), 3);
    lastBackup = catalogStore.findLatestBackup();
    lastBackupRoot = PersistedBlock.fromProto(lastBackup.getRootDirectory());
    catalogReader = new CatalogReader(blockStore, lastBackupRoot);
    fileSystemReader = new FileSystemReader<>(
        ImmutableSet.of(fs.getPath("/home/pmoor"), fs.getPath("/home/cmoor")), config.getSkippedPaths());
    backUpper = new BackUpper(catalogReader, splitter, blockStore);
    PersistedBlock newNewRoot = fileSystemReader.start(backUpper);
    System.out.println("new new root: " + newNewRoot);
    System.out.println(countingStorage);


    restore(blockStore, newNewRoot, fs.getPath("/", "tmp", "restore"));

    compare(fs.getPath("/", "home", "pmoor"), fs.getPath("/", "tmp", "restore", "home", "pmoor"));
    compare(fs.getPath("/", "home", "cmoor"), fs.getPath("/", "tmp", "restore", "home", "cmoor"));
  }

  private void compare(Path path1, Path path2) throws IOException {
    Preconditions.checkState(Files.exists(path1, LinkOption.NOFOLLOW_LINKS));
    boolean twoExists = Files.exists(path2, LinkOption.NOFOLLOW_LINKS);

    if (!twoExists) {
      System.out.println("missing: " + path1);
      return;
    }

    if (Files.isDirectory(path1, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.isDirectory(path2, LinkOption.NOFOLLOW_LINKS)) {
        System.out.println("not a directory: " + path2);
        return;
      }

      Iterator<Path> it = Files.list(path1).sorted().iterator();
      while (it.hasNext()) {
        Path path = path1.relativize(it.next());

        compare(path1.resolve(path), path2.resolve(path));
      }
    } else if (Files.isSymbolicLink(path1)) {
      if (!Files.isSymbolicLink(path2)) {
        System.out.println("not a symlink: " + path2);
        return;
      }

      Path target1 = Files.readSymbolicLink(path1);
      Path target2 = Files.readSymbolicLink(path2);
      if (!target1.equals(target2)) {
        System.out.printf("targets don't match for %s (%s): %s vs. %s\n", path1, path2, target1, target2);
        return;
      }
    } else if (Files.isRegularFile(path1, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.isRegularFile(path2, LinkOption.NOFOLLOW_LINKS)) {
        System.out.println("not a regular file: " + path2);
        return;
      }

      long size1 = Files.size(path1);
      long size2 = Files.size(path2);
      if (size1 != size2) {
        System.out.printf("file sizes don't match for %s (%s): %d vs. %d\n", path1, path2, size1, size2);
        return;
      }

    } else {
      System.out.println("unsupported type: " + path1);
    }
  }

  private void restore(BlockStore blockStore, PersistedBlock root, Path restoreRoot) throws IOException {
    Preconditions.checkState(!Files.exists(restoreRoot, LinkOption.NOFOLLOW_LINKS));
    Files.createDirectories(restoreRoot);

    Gletscher.Directory rootDir = Gletscher.Directory.parseFrom(Futures.getUnchecked(blockStore.retrieve(root)));
    restoreInner(blockStore, rootDir, restoreRoot);
  }

  private void restoreInner(BlockStore blockStore, Gletscher.Directory dir, Path path) throws IOException {
    for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
      switch (entry.getTypeCase()) {
        case FILE:
          OutputStream fos = new BufferedOutputStream(Files.newOutputStream(path.resolve(entry.getFile().getName()), StandardOpenOption.CREATE_NEW));
          for (Gletscher.PersistedBlock block : entry.getFile().getBlockList()) {
            byte[] data = Futures.getUnchecked(blockStore.retrieve(PersistedBlock.fromProto(block)));
            fos.write(data);
          }
          fos.close();
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

  private void createRandomFileTree(Path root, int maxLevels) throws IOException {
    Preconditions.checkState(!Files.exists(root, LinkOption.NOFOLLOW_LINKS));
    Files.createDirectories(root);

    List<String> names = new ArrayList<>();

    Random rnd = new Random();
    int numFiles = 1 + rnd.nextInt(10 - maxLevels);
    for (int i = 0; i < numFiles; i++) {
      String name = createRandomFileName(rnd);
      names.add(name);
      Path path = root.resolve(name);
      int size = rnd.nextInt(8 << 20);
      byte[] data = new byte[size];
      rnd.nextBytes(data);

      Files.write(path, data, StandardOpenOption.CREATE_NEW);
    }

    int numDirs = rnd.nextInt(maxLevels);
    for (int i = 0; i < numDirs; i++) {
      String name = createRandomDirectoryName(rnd);
      names.add(name);
      createRandomFileTree(root.resolve(name), maxLevels - 1);
    }

    int numSymlinks = rnd.nextInt(names.size());
    for (int i = 0; i < numSymlinks; i++) {
      String name = createRandomSymlinkName(rnd);
      names.add(name);

      String target = rnd.nextBoolean() ? names.get(rnd.nextInt(names.size())) : "../something/random";

      Files.createSymbolicLink(root.resolve(name), root.relativize(root.resolve(target)));
    }
  }

  private String createRandomSymlinkName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextLong()));
  }

  private String createRandomDirectoryName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextDouble()));
  }

  private String createRandomFileName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextInt()));
  }
}

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

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.testing.FileSystemTestRule;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class SpotCheckCommandTest {

  @Rule public FileSystemTestRule fsRule = FileSystemTestRule.unix();

  private TestCommandContext context;
  private GletscherMain main;
  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = fsRule.getFileSystem();
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    Files.write(
        fs.getPath("/config.properties"),
        ("version: 1\n"
                + "max_split_size: 65536\n"
                + "disable_cache: true\n"
                + "include:\n"
                + "  - /home\n")
            .getBytes(StandardCharsets.UTF_8));

    // files to back-up
    Files.createDirectories(fs.getPath("/home"));
    for (int i = 0; i < 20; i++) {
      String fileName = String.format("/home/file-%04d.txt", i);
      String contents = Strings.repeat(String.format("%04d", i), i * i * 1000);
      Files.newBufferedWriter(fs.getPath(fileName)).append(contents).close();
    }

    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);

    SpotCheckCommand.hashFn = Hashing.sha256();
  }

  @Test
  public void testSuccessfulSpotCheck() throws Exception {
    takeBackup();

    main.run("spot_check", "-c", "/config.properties", "--max_blocks", "10");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString())
        .startsWith("checking /home/file-0017.txt (offset 131072, 65536 bytes): success.");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testFailedSpotCheck() throws Exception {
    takeBackup();

    int i = 17;
    String fileName = String.format("/home/file-%04d.txt", i);
    String contents = Strings.repeat(String.format("%04d", i * i), i * i * 1000);
    Files.newBufferedWriter(fs.getPath(fileName)).append(contents).close();

    main.run("spot_check", "-c", "/config.properties", "--max_blocks", "10");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .startsWith("checking /home/file-0017.txt (offset 131072, 65536 bytes): failed!");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testSpotCheckBypassesCacheForDataButCachesMetadata() throws Exception {
    Path tempRoot = Files.createDirectories(fs.getPath("/spot-check-cache"));
    Path cacheDir = Files.createDirectories(tempRoot.resolve("cache"));
    Path homeDir = Files.createDirectories(tempRoot.resolve("home2"));
    for (int i = 0; i < 20; i++) {
      String contents = Strings.repeat(String.format("%04d", i), i * i * 1000);
      Files.writeString(homeDir.resolve(String.format("file-%04d.txt", i)), contents);
    }

    Path uncachedConfig = tempRoot.resolve("uncached.properties");
    Files.writeString(
        uncachedConfig,
        "version: 1\n"
            + "max_split_size: 65536\n"
            + "disable_cache: true\n"
            + "include:\n"
            + "  - "
            + homeDir.toAbsolutePath()
            + "\n");

    Path cachedConfig = tempRoot.resolve("cached.properties");
    Files.writeString(
        cachedConfig,
        "version: 1\n"
            + "max_split_size: 65536\n"
            + "disable_cache: false\n"
            + "cache_dir: "
            + cacheDir.toAbsolutePath()
            + "\n"
            + "include:\n"
            + "  - "
            + homeDir.toAbsolutePath()
            + "\n");

    // Backup directly to inMemoryStorage (no local cache populated yet).
    TestCommandContext backupContext = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(backupContext).run("backup", "-c", uncachedConfig.toString());
    assertThat(backupContext.status).isEqualTo(0);

    Set<String> dataObjects = inMemoryStorage.getFileNames(CloudFileStorage.ObjectType.DATA);
    Set<String> metaObjects = inMemoryStorage.getFileNames(CloudFileStorage.ObjectType.METADATA);

    List<String> fetchedObjects = new ArrayList<>();
    CloudFileStorage recordingStorage =
        new CloudFileStorage() {
          @Override
          public ListenableFuture<?> store(
              String name,
              byte[] data,
              HashCode md5,
              Map<String, String> metadata,
              StoreOptions options) {
            return inMemoryStorage.store(name, data, md5, metadata, options);
          }

          @Override
          public Iterator<FileHeader> listFiles(String prefix, int limit) {
            return inMemoryStorage.listFiles(prefix, limit);
          }

          @Override
          public ListenableFuture<Boolean> exists(String name) {
            return inMemoryStorage.exists(name);
          }

          @Override
          public ListenableFuture<byte[]> get(String name, GetOptions options) {
            fetchedObjects.add(name);
            return inMemoryStorage.get(name, options);
          }

          @Override
          public void close() {
            inMemoryStorage.close();
          }
        };

    // First spot_check warms metadata cache, and fetches 10 sampled data blocks from storage.
    TestCommandContext ctx1 = new TestCommandContext(fs, recordingStorage);
    new GletscherMain(ctx1)
        .run("spot_check", "-c", cachedConfig.toString(), "--max_blocks", "10");
    assertThat(ctx1.status).isEqualTo(0);
    assertThat(fetchedObjects)
        .containsAtLeastElementsIn(
            metaObjects.stream().filter(n -> n.startsWith("blocks/")).toList());
    assertThat(fetchedObjects.stream().filter(dataObjects::contains).count()).isEqualTo(10);

    // Second spot_check reads metadata from cache (0 remote metadata fetches),
    // but still fetches all 10 sampled data blocks from remote storage (cache bypassed).
    fetchedObjects.clear();
    TestCommandContext ctx2 = new TestCommandContext(fs, recordingStorage);
    new GletscherMain(ctx2)
        .run("spot_check", "-c", cachedConfig.toString(), "--max_blocks", "10");
    assertThat(ctx2.status).isEqualTo(0);
    assertThat(fetchedObjects.stream().filter(metaObjects::contains).count()).isEqualTo(0);
    assertThat(fetchedObjects.stream().filter(dataObjects::contains).count()).isEqualTo(10);
  }

  private void takeBackup() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }
}

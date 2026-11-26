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

import com.google.common.hash.HashCode;
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

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class PurgeCacheCommandTest {

  @Rule public FileSystemTestRule fsRule = FileSystemTestRule.unix();

  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;
  private Path tempRoot;
  private Path cacheDir;
  private Path homeDir;
  private Path cachedConfig;
  private Path uncachedConfig;

  @Before
  public void setUp() throws Exception {
    fs = fsRule.getFileSystem();
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    tempRoot = Files.createDirectories(fs.getPath("/purge-cache-cmd"));
    cacheDir = Files.createDirectories(tempRoot.resolve("cache"));
    homeDir = Files.createDirectories(tempRoot.resolve("home"));

    cachedConfig = tempRoot.resolve("cached.properties");
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

    uncachedConfig = tempRoot.resolve("uncached.properties");
    Files.writeString(
        uncachedConfig,
        "version: 1\n"
            + "max_split_size: 65536\n"
            + "disable_cache: true\n"
            + "include:\n"
            + "  - "
            + homeDir.toAbsolutePath()
            + "\n");
  }

  @Test
  public void testPurgeRemovesDataAndOldSnapshotMetadataAcrossSnapshots() throws Exception {
    // Snapshot 1: /home/old-only.txt
    Files.writeString(homeDir.resolve("old-only.txt"), "snapshot-1-unique-payload");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"), cachedConfig);

    // Snapshot 2: replace with /home/new.txt
    Files.delete(homeDir.resolve("old-only.txt"));
    Files.writeString(homeDir.resolve("new.txt"), "snapshot-2-unique-payload");
    takeBackupAt(Instant.parse("2026-01-02T00:00:00Z"), cachedConfig);

    // Restore populates cache.mv with data payloads as well as metadata.
    Path restoreDir = tempRoot.resolve("restore");
    TestCommandContext restoreCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(restoreCtx)
        .run("restore", "-c", cachedConfig.toString(), restoreDir.toString());
    assertThat(restoreCtx.status).isEqualTo(0);

    // Populate cached prefix listing via list_objects --type data.
    TestCommandContext listCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(listCtx).run("list_objects", "-c", cachedConfig.toString(), "--type", "data");
    assertThat(listCtx.status).isEqualTo(0);

    // purge_cache with --snapshots all removes data blocks (from restore) and cached listings
    // while keeping all metadata blocks from both snapshots.
    TestCommandContext purgeAllCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(purgeAllCtx)
        .run("purge_cache", "-c", cachedConfig.toString(), "--snapshots", "all");
    assertThat(purgeAllCtx.status).isEqualTo(0);
    assertThat(purgeAllCtx.stdOutString()).contains("Purged 1 cached object(s) (");
    assertThat(purgeAllCtx.stdOutString()).contains("and 17 cached listing(s);");
    assertThat(purgeAllCtx.stdOutString()).contains("from 2 snapshot(s).");
    assertThat(purgeAllCtx.stdOutString()).doesNotContain("exists marker");
    assertThat(purgeAllCtx.stdErrString()).isEmpty();

    // Default purge_cache (-n 1) now purges snapshot 1's obsolete metadata blocks while keeping
    // snapshot 2's metadata blocks.
    TestCommandContext purgeLatestCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(purgeLatestCtx).run("purge_cache", "-c", cachedConfig.toString());
    assertThat(purgeLatestCtx.status).isEqualTo(0);
    assertThat(purgeLatestCtx.stdOutString()).contains("Purged ");
    assertThat(purgeLatestCtx.stdOutString()).doesNotContain("Purged 0 cached object(s)");
    assertThat(purgeLatestCtx.stdOutString()).contains("from 1 snapshot(s).");
    assertThat(purgeLatestCtx.stdErrString()).isEmpty();

    // Re-running purge_cache -n 1 immediately reports 0 objects purged (1 listing from its own
    // backups/ scan before clearing listingsMap).
    TestCommandContext secondPurgeCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(secondPurgeCtx).run("purge_cache", "-c", cachedConfig.toString());
    assertThat(secondPurgeCtx.status).isEqualTo(0);
    assertThat(secondPurgeCtx.stdOutString())
        .contains("Purged 0 cached object(s) (0 B) and 1 cached listing(s);");

    // Running stats -n 1 against a recording storage verifies snapshot 2's metadata is still
    // served completely from cache (0 remote get calls), while snapshot 1's metadata is no
    // longer cached (stats -n 2 requires remote get calls for snapshot 1's metadata).
    List<String> remoteGets = new ArrayList<>();
    CloudFileStorage recordingStorage = recordingStorage(remoteGets, new ArrayList<>());

    TestCommandContext stats1Ctx = new TestCommandContext(fs, recordingStorage);
    new GletscherMain(stats1Ctx).run("stats", "-c", cachedConfig.toString(), "-n", "1");
    assertThat(stats1Ctx.status).isEqualTo(0);
    assertThat(remoteGets).isEmpty();

    TestCommandContext stats2Ctx = new TestCommandContext(fs, recordingStorage);
    new GletscherMain(stats2Ctx).run("stats", "-c", cachedConfig.toString(), "-n", "2");
    assertThat(stats2Ctx.status).isEqualTo(0);
    assertThat(remoteGets).isNotEmpty();

    // Verify cached listings were purged: list_objects --max-listing-age 1h must still list live.
    List<String> remoteLists = new ArrayList<>();
    CloudFileStorage listRecordingStorage = recordingStorage(new ArrayList<>(), remoteLists);
    TestCommandContext cachedListCtx = new TestCommandContext(fs, listRecordingStorage);
    new GletscherMain(cachedListCtx)
        .run(
            "list_objects",
            "-c",
            cachedConfig.toString(),
            "--type",
            "data",
            "--max-listing-age",
            "1h");
    assertThat(cachedListCtx.status).isEqualTo(0);
    assertThat(remoteLists).isNotEmpty();

    // Adding a duplicate file in a new backup causes exists() to return true and populate existsMap.
    Files.writeString(homeDir.resolve("copy-of-new.txt"), "snapshot-2-unique-payload");
    takeBackupAt(Instant.parse("2026-01-03T00:00:00Z"), cachedConfig);

    // Running purge_cache --purge-exists removes exists markers for data blocks & old snapshots.
    TestCommandContext purgeExistsCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(purgeExistsCtx)
        .run("purge_cache", "-c", cachedConfig.toString(), "--purge-exists");
    assertThat(purgeExistsCtx.status).isEqualTo(0);
    assertThat(purgeExistsCtx.stdOutString()).contains("Purged 1 cached exists marker(s);");
  }

  @Test
  public void testPurgeWithCacheDisabledOrNoBackupsFails() throws Exception {
    TestCommandContext disabledCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(disabledCtx).run("purge_cache", "-c", uncachedConfig.toString());
    assertThat(disabledCtx.status).isEqualTo(-1);
    assertThat(disabledCtx.stdErrString()).contains("Local cache is disabled");

    TestCommandContext noBackupsCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(noBackupsCtx).run("purge_cache", "-c", cachedConfig.toString());
    assertThat(noBackupsCtx.status).isEqualTo(-1);
    assertThat(noBackupsCtx.stdErrString()).contains("No existing backups found");
  }

  @Test
  public void testInvalidUsage() throws Exception {
    TestCommandContext argCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(argCtx).run("purge_cache", "-c", cachedConfig.toString(), "extra");
    assertThat(argCtx.status).isEqualTo(-1);
    assertThat(argCtx.stdErrString()).contains("Command does not accept arguments");

    TestCommandContext zeroCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(zeroCtx)
        .run("purge_cache", "-c", cachedConfig.toString(), "--snapshots", "0");
    assertThat(zeroCtx.status).isEqualTo(-1);
    assertThat(zeroCtx.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    TestCommandContext badSnapshotsCtx = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(badSnapshotsCtx)
        .run("purge_cache", "-c", cachedConfig.toString(), "-n", "bogus");
    assertThat(badSnapshotsCtx.status).isEqualTo(-1);
    assertThat(badSnapshotsCtx.stdErrString()).contains("Invalid --snapshots value");
  }

  private CloudFileStorage recordingStorage(List<String> remoteGets, List<String> remoteLists) {
    return new CloudFileStorage() {
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
        remoteLists.add(prefix);
        return inMemoryStorage.listFiles(prefix, limit);
      }

      @Override
      public ListenableFuture<Boolean> exists(String name) {
        return inMemoryStorage.exists(name);
      }

      @Override
      public ListenableFuture<byte[]> get(String name, GetOptions options) {
        remoteGets.add(name);
        return inMemoryStorage.get(name, options);
      }

      @Override
      public void close() {
        inMemoryStorage.close();
      }
    };
  }

  private void takeBackupAt(Instant when, Path configPath) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    context.clock = Clock.fixed(when, ZoneOffset.UTC);
    new GletscherMain(context).run("backup", "-c", configPath.toString());
    assertThat(context.status).isEqualTo(0);
  }
}

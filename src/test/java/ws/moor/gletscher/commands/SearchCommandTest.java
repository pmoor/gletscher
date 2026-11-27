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

import com.google.common.hash.HashCode;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.catalog.CatalogPath;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.catalog.CatalogReaders;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CompressingCloudFileStorage;
import ws.moor.gletscher.cloud.EncryptingCloudFileStorage;
import ws.moor.gletscher.cloud.SigningCloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.testing.FileSystemTestRule;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class SearchCommandTest {

  @ClassRule public static FileSystemTestRule fs = FileSystemTestRule.unix();
  private static InMemoryCloudFileStorage inMemoryStorage;

  @BeforeClass
  public static void setUp() throws Exception {
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    fs.writeFile("/config.properties", """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """);

    fs.createDirectories("/home");
    fs.writeFile("/home/a.txt", "A");
    fs.writeFile("/home/b.pdf", "AA");
    fs.writeFile("/home/c.jpg", "AAA");

    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);

    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }

  @Test
  public void testSearchWithNoArguments() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).containsMatch("/home/a.txt: .* \\(1 bytes\\)");
    assertThat(context.stdOutString()).containsMatch("/home/b.pdf: .* \\(2 bytes\\)");
    assertThat(context.stdOutString()).containsMatch("/home/c.jpg: .* \\(3 bytes\\)");
  }

  @Test
  public void testSearchWithRegexp() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties", "--", "(a|b)\\.(txt|pdf)", "-b\\.pdf");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("/home/a.txt");
    assertThat(context.stdOutString()).doesNotContain("/home/b.pdf");
    assertThat(context.stdOutString()).doesNotContain("/home/c.jpg");
  }

  @Test
  public void testSearchFullName() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties", "^/home/c.jpg$");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("/home/c.jpg");
    assertThat(context.stdOutString()).doesNotContain("/home/a.txt");
    assertThat(context.stdOutString()).doesNotContain("/home/b.pdf");
  }

  @Test
  public void testSearchNotFoundReturnsMinusOne() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties", "nonexistent");

    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
  }

  @Test
  public void testSearchEmptyStoreReturnsMinusOne() throws Exception {
    FileSystem emptyFs = Jimfs.newFileSystem(unix());
    Files.writeString(emptyFs.getPath("/config.properties"), "version: 1\ndisable_cache: true\n");
    InMemoryCloudFileStorage emptyStorage =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    TestCommandContext context = new TestCommandContext(emptyFs, emptyStorage);
    new GletscherMain(context).run("search", "-c", "/config.properties", "anything");

    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
  }

  @Test
  public void testSearchMultipleSnapshotsAbortAndAll() throws Exception {
    FileSystem localFs = Jimfs.newFileSystem(unix());
    InMemoryCloudFileStorage storage =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    Files.writeString(
        localFs.getPath("/config.properties"),
        """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """);
    Files.createDirectories(localFs.getPath("/home"));

    // Snapshot 1: shared.txt + snap1-only.txt
    Files.writeString(localFs.getPath("/home/shared.txt"), "v1");
    Files.writeString(localFs.getPath("/home/snap1-only.txt"), "old");
    takeBackupAt(localFs, storage, Instant.parse("2024-01-01T00:00:00Z"));

    // Snapshot 2: shared.txt + snap2-only.txt (snap1-only.txt deleted)
    Files.delete(localFs.getPath("/home/snap1-only.txt"));
    Files.writeString(localFs.getPath("/home/shared.txt"), "v2");
    Files.writeString(localFs.getPath("/home/snap2-only.txt"), "mid");
    takeBackupAt(localFs, storage, Instant.parse("2025-01-01T00:00:00Z"));

    // Snapshot 3 (latest): shared.txt only
    Files.delete(localFs.getPath("/home/snap2-only.txt"));
    Files.writeString(localFs.getPath("/home/shared.txt"), "v3");
    takeBackupAt(localFs, storage, Instant.parse("2026-01-01T00:00:00Z"));

    // Default: aborts after the first snapshot with a match (only 1 "Backup " header for shared.txt).
    TestCommandContext defaultCtx = runSearch(localFs, storage, "-c", "/config.properties", "shared");
    assertThat(defaultCtx.status).isEqualTo(0);
    assertThat(countOccurrences(defaultCtx.stdOutString(), "Backup ")).isEqualTo(1);

    // -a / --all: searches through all snapshots.
    TestCommandContext allCtx =
        runSearch(localFs, storage, "-c", "/config.properties", "-a", "shared");
    assertThat(allCtx.status).isEqualTo(0);
    assertThat(countOccurrences(allCtx.stdOutString(), "Backup ")).isEqualTo(3);

    // -n 2 --all: searches only the last 2 snapshots.
    TestCommandContext lastTwoAllCtx =
        runSearch(localFs, storage, "-c", "/config.properties", "-n", "2", "--all", "shared");
    assertThat(lastTwoAllCtx.status).isEqualTo(0);
    assertThat(countOccurrences(lastTwoAllCtx.stdOutString(), "Backup ")).isEqualTo(2);

    // Default falls back to older snapshots when not in latest, then stops once found.
    TestCommandContext deletedFileCtx =
        runSearch(localFs, storage, "-c", "/config.properties", "snap1-only");
    assertThat(deletedFileCtx.status).isEqualTo(0);
    assertThat(countOccurrences(deletedFileCtx.stdOutString(), "Backup ")).isEqualTo(1);
    assertThat(deletedFileCtx.stdOutString()).contains("/home/snap1-only.txt");

    // -n 2 skips snapshot 1, so snap1-only.txt is not found -> returns -1.
    TestCommandContext boundedNotFoundCtx =
        runSearch(localFs, storage, "-c", "/config.properties", "-n", "2", "snap1-only");
    assertThat(boundedNotFoundCtx.status).isEqualTo(-1);
    assertThat(boundedNotFoundCtx.stdOutString()).isEmpty();

    // --snapshots all finds snap1-only.txt too.
    TestCommandContext snapshotsAllCtx =
        runSearch(localFs, storage, "-c", "/config.properties", "--snapshots", "all", "snap1-only");
    assertThat(snapshotsAllCtx.status).isEqualTo(0);
    assertThat(snapshotsAllCtx.stdOutString()).contains("/home/snap1-only.txt");
  }

  @Test
  public void testInvalidSnapshotsAndPatternUsage() throws Exception {
    TestCommandContext zeroSnapshots =
        runSearch(fs.getFileSystem(), inMemoryStorage, "-c", "/config.properties", "-n", "0");
    assertThat(zeroSnapshots.status).isEqualTo(-1);
    assertThat(zeroSnapshots.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    TestCommandContext badSnapshots =
        runSearch(fs.getFileSystem(), inMemoryStorage, "-c", "/config.properties", "-n", "bogus");
    assertThat(badSnapshots.status).isEqualTo(-1);
    assertThat(badSnapshots.stdErrString()).contains("Invalid --snapshots value");

    TestCommandContext badPattern =
        runSearch(fs.getFileSystem(), inMemoryStorage, "-c", "/config.properties", "[unclosed");
    assertThat(badPattern.status).isEqualTo(-1);
    assertThat(badPattern.stdErrString()).contains("Invalid pattern");
  }

  @Test
  public void testSkipsReFetchingUnchangedDirectoriesAcrossSnapshots() throws Exception {
    FileSystem localFs = Jimfs.newFileSystem(unix());
    InMemoryCloudFileStorage backing =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    Map<String, Integer> getCounts = new HashMap<>();
    CloudFileStorage countingStorage = countingGetStorage(backing, getCounts);

    Files.writeString(
        localFs.getPath("/config.properties"),
        """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """);
    Files.createDirectories(localFs.getPath("/home/unchanged/nested"));
    Files.writeString(localFs.getPath("/home/unchanged/nested/keep.txt"), "keep");
    Files.writeString(localFs.getPath("/home/unchanged/other.txt"), "other");
    Files.writeString(localFs.getPath("/home/changing.txt"), "v1");
    takeBackupAt(localFs, backing, Instant.parse("2024-01-01T00:00:00Z"));

    Files.writeString(localFs.getPath("/home/changing.txt"), "v2");
    takeBackupAt(localFs, backing, Instant.parse("2025-01-01T00:00:00Z"));

    Files.writeString(localFs.getPath("/home/changing.txt"), "v3");
    takeBackupAt(localFs, backing, Instant.parse("2026-01-01T00:00:00Z"));

    Configuration config = Configuration.fromFile(localFs.getPath("/config.properties"));
    CloudFileStorage transformedBacking =
        new CompressingCloudFileStorage(
            new EncryptingCloudFileStorage(
                new SigningCloudFileStorage(backing, new Signer(config.getSigningKey())),
                new Cryptor(config.getEncryptionKey(), config.getSigningKey())),
            new Compressor());
    BlockStore blockStore =
        new BlockStore(transformedBacking, new Signer(config.getSigningKey()));
    CatalogStore catalogStore = new CatalogStore(localFs, transformedBacking, blockStore);
    CatalogReader latestReader =
        CatalogReaders.fromBlockStore(blockStore, catalogStore.getLatestCatalog().orElseThrow());
    String unchangedDirObj =
        BlockStore.toFileName(
            latestReader
                .findDirectory(CatalogPath.fromHumanReadableString("/home/unchanged"))
                .getAddress());
    String nestedDirObj =
        BlockStore.toFileName(
            latestReader
                .findDirectory(CatalogPath.fromHumanReadableString("/home/unchanged/nested"))
                .getAddress());

    // 1) Searching for a non-existent file across all 3 snapshots only fetches unchanged dir blocks once.
    getCounts.clear();
    TestCommandContext notFoundCtx =
        runSearch(localFs, countingStorage, "-c", "/config.properties", "does-not-exist");
    assertThat(notFoundCtx.status).isEqualTo(-1);
    assertThat(getCounts).containsEntry(unchangedDirObj, 1);
    assertThat(getCounts).containsEntry(nestedDirObj, 1);

    // 2) Searching with -a for a file inside /home/unchanged still prints all 3 snapshots,
    //    while fetching the unchanged directory blocks only once.
    getCounts.clear();
    TestCommandContext allMatchCtx =
        runSearch(localFs, countingStorage, "-c", "/config.properties", "-a", "keep\\.txt");
    assertThat(allMatchCtx.status).isEqualTo(0);
    assertThat(countOccurrences(allMatchCtx.stdOutString(), "Backup ")).isEqualTo(3);
    assertThat(countOccurrences(allMatchCtx.stdOutString(), "/home/unchanged/nested/keep.txt"))
        .isEqualTo(3);
    assertThat(getCounts).containsEntry(unchangedDirObj, 1);
    assertThat(getCounts).containsEntry(nestedDirObj, 1);
  }

  @Test
  public void testIdenticalDirectoriesAtDifferentPathsStillMatchPathSpecificPattern()
      throws Exception {
    FileSystem localFs = Jimfs.newFileSystem(unix());
    InMemoryCloudFileStorage storage =
        new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    Files.writeString(
        localFs.getPath("/config.properties"),
        """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """);
    Files.createDirectories(localFs.getPath("/home/dir1"));
    Files.createDirectories(localFs.getPath("/home/dir2"));
    Files.writeString(localFs.getPath("/home/dir1/target.txt"), "same-content");
    Files.writeString(localFs.getPath("/home/dir2/target.txt"), "same-content");
    takeBackupAt(localFs, storage, Instant.parse("2026-01-01T00:00:00Z"));

    TestCommandContext ctx =
        runSearch(localFs, storage, "-c", "/config.properties", "^/home/dir2/target\\.txt$");
    assertThat(ctx.status).isEqualTo(0);
    assertThat(ctx.stdOutString()).contains("/home/dir2/target.txt");
    assertThat(ctx.stdOutString()).doesNotContain("/home/dir1/target.txt");
  }

  private static CloudFileStorage countingGetStorage(
      CloudFileStorage delegate, Map<String, Integer> getCounts) {
    return new CloudFileStorage() {
      @Override
      public ListenableFuture<?> store(
          String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
        return delegate.store(name, data, md5, metadata, options);
      }

      @Override
      public Iterator<FileHeader> listFiles(String prefix, int limit) {
        return delegate.listFiles(prefix, limit);
      }

      @Override
      public ListenableFuture<Boolean> exists(String name) {
        return delegate.exists(name);
      }

      @Override
      public ListenableFuture<byte[]> get(String name, GetOptions options) {
        getCounts.merge(name, 1, Integer::sum);
        return delegate.get(name, options);
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }

  private static void takeBackupAt(
      FileSystem localFs, CloudFileStorage storage, Instant when) throws Exception {
    TestCommandContext context = new TestCommandContext(localFs, storage);
    context.clock = Clock.fixed(when, ZoneOffset.UTC);
    new GletscherMain(context).run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }

  private static TestCommandContext runSearch(
      FileSystem localFs, CloudFileStorage storage, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(localFs, storage);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "search";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    new GletscherMain(context).run(fullArgs);
    return context;
  }

  private static int countOccurrences(String text, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = text.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }
}

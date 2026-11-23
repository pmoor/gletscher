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

import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.SigningCloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.proto.testing.Testing;
import ws.moor.gletscher.util.Signer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class ListObjectsCommandTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private FileSystem fs;
  private InMemoryCloudFileStorage storage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem(unix());
    storage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    Files.write(
        fs.getPath("/config.properties"),
        ("version: 1\n"
                + "max_split_size: 65536\n"
                + "disable_cache: true\n"
                + "gcs:\n"
                + "  bucket_name: my-backup-bucket\n"
                + "  object_prefix: prod/\n"
                + "include:\n"
                + "  - /home\n")
            .getBytes(StandardCharsets.UTF_8));

    Files.createDirectories(fs.getPath("/home/sub"));
    Files.writeString(fs.getPath("/home/a.txt"), "small-a");
    Files.writeString(fs.getPath("/home/sub/b.bin"), "medium-data-block-content-1234567890");
  }

  @Test
  public void emptyStoreProducesNoOutput() throws Exception {
    assertThat(runListObjects("-c", "/config.properties")).isEmpty();
    assertThat(runListObjects("-c", "/config.properties", "--type", "metadata", "--snapshots", "3"))
        .isEmpty();
    assertThat(runListObjects("-c", "/config.properties", "--type", "data")).isEmpty();
  }

  @Test
  public void listMetadataAndDataAcrossSnapshots() throws Exception {
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));
    Set<String> metaAfterSnap1 = storage.getFileNames(CloudFileStorage.ObjectType.METADATA);
    Set<String> dataAfterSnap1 = storage.getFileNames(CloudFileStorage.ObjectType.DATA);

    Files.writeString(fs.getPath("/home/c.txt"), "snapshot-2-new-file-content-abcdefghijklmnopqrstuvwxyz");
    takeBackupAt(Instant.parse("2025-06-01T00:00:00Z"));
    Set<String> metaAfterSnap2 = storage.getFileNames(CloudFileStorage.ObjectType.METADATA);
    String snap2Pointer =
        Sets.difference(metaAfterSnap2, metaAfterSnap1)
            .stream()
            .filter(n -> n.startsWith("backups/"))
            .findFirst()
            .orElseThrow();

    Files.writeString(fs.getPath("/home/sub/d.txt"), "snapshot-3-newest-file");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    Set<String> allMeta = storage.getFileNames(CloudFileStorage.ObjectType.METADATA);
    Set<String> allData = storage.getFileNames(CloudFileStorage.ObjectType.DATA);
    String snap1Pointer =
        metaAfterSnap1.stream().filter(n -> n.startsWith("backups/")).findFirst().orElseThrow();
    String snap3Pointer =
        Sets.difference(allMeta, metaAfterSnap2)
            .stream()
            .filter(n -> n.startsWith("backups/"))
            .findFirst()
            .orElseThrow();
    assertThat(allData).hasSize(4);

    // Default (--type all) lists everything sorted alphabetically.
    List<String> allListed = runListObjects("-c", "/config.properties");
    assertThat(allListed).containsExactlyElementsIn(Sets.union(allMeta, allData));
    assertThat(allListed).isInOrder();

    // --type metadata lists all metadata objects.
    List<String> allMetaListed = runListObjects("-c", "/config.properties", "--type", "metadata");
    assertThat(allMetaListed).containsExactlyElementsIn(allMeta);
    assertThat(allMetaListed).isInOrder();

    // --type metadata --snapshots 1 excludes objects unique to snapshots 1 and 2.
    List<String> snap1OnlyListed =
        runListObjects("-c", "/config.properties", "--type", "metadata", "--snapshots", "1");
    assertThat(snap1OnlyListed).contains(snap3Pointer);
    assertThat(snap1OnlyListed).doesNotContain(snap1Pointer);
    assertThat(snap1OnlyListed).doesNotContain(snap2Pointer);
    assertThat(snap1OnlyListed.size()).isLessThan(allMeta.size());

    // --type metadata --snapshots 2 covers snapshots 2 and 3, strictly between 1 and all.
    List<String> snap2Listed =
        runListObjects("-c", "/config.properties", "--type", "metadata", "--snapshots", "2");
    assertThat(snap2Listed.size()).isGreaterThan(snap1OnlyListed.size());
    assertThat(snap2Listed.size()).isLessThan(allMeta.size());
    assertThat(snap2Listed).containsAtLeastElementsIn(snap1OnlyListed);
    assertThat(snap2Listed).contains(snap2Pointer);
    assertThat(snap2Listed).contains(snap3Pointer);
    assertThat(snap2Listed).doesNotContain(snap1Pointer);

    // --type metadata --snapshots 3 matches all metadata objects.
    assertThat(runListObjects("-c", "/config.properties", "--type", "metadata", "--snapshots", "3"))
        .containsExactlyElementsIn(allMeta);

    // --type data excludes all metadata objects and includes only data blocks.
    List<String> dataListed = runListObjects("-c", "/config.properties", "--type", "data");
    assertThat(dataListed).containsExactlyElementsIn(allData);
    assertThat(dataListed).isInOrder();
  }

  @Test
  public void filterBySizeAgeStorageClassAndGsUri() throws Exception {
    // Upload a large incompressible file so its stored block exceeds 1KiB.
    byte[] bigData = new byte[4096];
    new Random(42).nextBytes(bigData);
    Files.write(fs.getPath("/home/large.bin"), bigData);

    Instant now = Instant.parse("2026-06-01T00:00:00Z");
    takeBackupAt(Instant.parse("2026-05-01T00:00:00Z"));

    List<String> dataObjects = runListObjects("-c", "/config.properties", "--type", "data");
    assertThat(dataObjects).hasSize(3);

    // Find headers so we can assign distinct storage classes & creation timestamps cleanly.
    String largeBlock =
        dataObjects.stream().filter(n -> n.endsWith(":4096")).findFirst().orElseThrow();
    List<String> smallBlocks =
        dataObjects.stream().filter(n -> !n.endsWith(":4096")).collect(Collectors.toList());
    assertThat(smallBlocks).hasSize(2);

    storage.setCreatedTime(largeBlock, now.minusSeconds(200L * 86400)); // 200d old
    storage.setStorageClass(largeBlock, "MULTI_REGIONAL");

    storage.setCreatedTime(smallBlocks.get(0), now.minusSeconds(120L * 86400)); // 120d old
    storage.setStorageClass(smallBlocks.get(0), "STANDARD");

    storage.setCreatedTime(smallBlocks.get(1), now.minusSeconds(10L * 86400)); // 10d old
    storage.setStorageClass(smallBlocks.get(1), "ARCHIVE");

    // Size filters.
    assertThat(
            runListObjectsAt(now, "-c", "/config.properties", "--type", "data", "--min-size", "1KiB"))
        .containsExactly(largeBlock);
    assertThat(
            runListObjectsAt(now, "-c", "/config.properties", "--type", "data", "--max-size", "1KiB"))
        .containsExactlyElementsIn(smallBlocks);

    // Age filters.
    assertThat(
            runListObjectsAt(now, "-c", "/config.properties", "--type", "data", "--min-age", "90d"))
        .containsExactly(largeBlock, smallBlocks.get(0));
    assertThat(
            runListObjectsAt(now, "-c", "/config.properties", "--type", "data", "--max-age", "150d", "--min-age", "30d"))
        .containsExactly(smallBlocks.get(0));

    // Storage class include/exclude filters (case-insensitive & multi-value).
    assertThat(
            runListObjectsAt(
                now,
                "-c",
                "/config.properties",
                "--type",
                "data",
                "--storage-class",
                "multi_regional,archive"))
        .containsExactly(largeBlock, smallBlocks.get(1));

    ListingResult multiClass =
        runListObjectsDetailed(
            now,
            "-c",
            "/config.properties",
            "--type",
            "data",
            "--min-age",
            "90d",
            "--exclude-storage-class",
            "ARCHIVE",
            "--gs-uri");
    assertThat(multiClass.lines())
        .containsExactly(
            "gs://my-backup-bucket/prod/" + largeBlock,
            "gs://my-backup-bucket/prod/" + smallBlocks.get(0));
    assertThat(multiClass.stdErr()).contains("MULTI_REGIONAL: 1 object, ");
    assertThat(multiClass.stdErr()).contains("STANDARD: 1 object, ");
    assertThat(multiClass.stdErr()).contains("Total: 2 objects, ");

    // Single storage class emits that class line without a redundant Total line.
    ListingResult singleClass =
        runListObjectsDetailed(
            now,
            "-c",
            "/config.properties",
            "--type",
            "data",
            "--storage-class",
            "ARCHIVE");
    assertThat(singleClass.lines()).containsExactly(smallBlocks.get(1));
    assertThat(singleClass.stdErr()).contains("ARCHIVE: 1 object, ");
    assertThat(singleClass.stdErr()).doesNotContain("Total:");

    // Unfiltered --type metadata omits size/class breakdown and emits object count.
    ListingResult metadataOnly =
        runListObjectsDetailed(now, "-c", "/config.properties", "--type", "metadata");
    assertThat(metadataOnly.lines()).isNotEmpty();
    assertThat(metadataOnly.stdErr())
        .contains(metadataOnly.lines().size() + " objects (sizes and storage classes omitted");
  }

  @Test
  public void listOrphanedObjectsAcrossSnapshotsAndAbortedUploads() throws Exception {
    Instant now = Instant.parse("2026-06-01T00:00:00Z");

    // Take snapshot 1 with a file that will later be deleted.
    Files.writeString(fs.getPath("/home/ephemeral.txt"), "only-in-snapshot-1-unique-payload");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));
    Set<String> snap1Meta = storage.getFileNames(CloudFileStorage.ObjectType.METADATA);
    Set<String> snap1Data = storage.getFileNames(CloudFileStorage.ObjectType.DATA);

    // Delete ephemeral.txt and take snapshot 2.
    Files.delete(fs.getPath("/home/ephemeral.txt"));
    takeBackupAt(Instant.parse("2026-05-01T00:00:00Z"));
    Set<String> snap2MetaOnly =
        Sets.difference(storage.getFileNames(CloudFileStorage.ObjectType.METADATA), snap1Meta);
    Set<String> allValidObjects =
        Sets.union(
            storage.getFileNames(CloudFileStorage.ObjectType.METADATA),
            storage.getFileNames(CloudFileStorage.ObjectType.DATA));

    // Simulate an aborted upload: stray signed blocks uploaded without ever linking a catalog.
    Signer signer =
        new Signer(Configuration.fromFile(fs.getPath("/config.properties")).getSigningKey());
    BlockStore blockStore =
        new BlockStore(new SigningCloudFileStorage(storage, signer), signer);
    byte[] payload1 = "aborted-data-1".getBytes(StandardCharsets.UTF_8);
    byte[] payload2 = "aborted-data-2".getBytes(StandardCharsets.UTF_8);
    String oldAbortedBlock =
        BlockStore.toFileName(
            blockStore.store(payload1, false, CloudFileStorage.ObjectType.DATA).get());
    String recentAbortedBlock =
        BlockStore.toFileName(
            blockStore.store(payload2, false, CloudFileStorage.ObjectType.DATA).get());
    storage.setCreatedTime(oldAbortedBlock, now.minusSeconds(30L * 86400));
    storage.setCreatedTime(recentAbortedBlock, now.minusSeconds(3600)); // 1h old

    // With --snapshots all (and default for --type orphaned), only unreferenced aborted blocks show up.
    assertThat(runListObjectsAt(now, "-c", "/config.properties", "--type", "orphaned"))
        .containsExactly(oldAbortedBlock, recentAbortedBlock);
    assertThat(
            runListObjectsAt(
                now, "-c", "/config.properties", "--type", "orphaned", "--snapshots", "all"))
        .containsExactly(oldAbortedBlock, recentAbortedBlock);
    assertThat(
            runListObjectsAt(
                now,
                "-c",
                "/config.properties",
                "--type",
                "orphaned",
                "--snapshots",
                "all",
                "--min-age",
                "7d",
                "--gs-uri"))
        .containsExactly("gs://my-backup-bucket/prod/" + oldAbortedBlock);

    // Also verify --snapshots all works for --type metadata.
    assertThat(
            runListObjectsAt(
                now, "-c", "/config.properties", "--type", "metadata", "--snapshots", "all"))
        .containsExactlyElementsIn(storage.getFileNames(CloudFileStorage.ObjectType.METADATA));

    // With --snapshots 1, orphaned includes aborted blocks + objects unique to snapshot 1,
    // while keeping shared data blocks (/home/a.txt, /home/sub/b.bin) and snapshot 2 metadata!
    for (String snap2Obj : Sets.union(snap2MetaOnly, snap1Data)) {
      storage.setCreatedTime(snap2Obj, now.minusSeconds(10L * 86400));
    }
    List<String> orphanedExceptLast1 =
        runListObjectsAt(now, "-c", "/config.properties", "--type", "orphaned", "--snapshots", "1");
    assertThat(orphanedExceptLast1).containsAtLeast(oldAbortedBlock, recentAbortedBlock);
    assertThat(orphanedExceptLast1).containsNoneIn(snap2MetaOnly);
    assertThat(orphanedExceptLast1.size()).isGreaterThan(2);
    // Ephemeral file's unique data block is no longer referenced by snapshot 2, so it IS orphaned:
    Set<String> allInStore =
        Sets.union(allValidObjects, Set.of(oldAbortedBlock, recentAbortedBlock));
    assertThat(allInStore).containsAtLeastElementsIn(orphanedExceptLast1);
  }

  @Test
  public void legacy2016UnlinkedSnapshotsAreCollectedUnderAll() throws Exception {
    try (InputStream resource = getClass().getResourceAsStream("/20160930.bin")) {
      assertThat(resource).isNotNull();
      storage.mergeFromProto(Testing.FileList.parseFrom(resource));
    }

    // Add a modern 2026 snapshot on top of the 2016 repository.
    // BackupCommand calls CatalogStore.getLatestCatalog(), which also materializes the v0
    // 2016 catalog as a block in blocks/ and references it via the 2026 catalog's base_catalog.
    takeBackupAt(Instant.parse("2026-06-01T00:00:00Z"));

    // --snapshots all collects both the legacy 2016 inline catalog (including its materialized
    // base_catalog block) and the modern 2026 catalog.
    assertThat(
            runListObjects("-c", "/config.properties", "--type", "orphaned", "--snapshots", "all"))
        .isEmpty();
  }

  @Test
  public void directoryPreviousVersionLinksPreserveHistoryWhenCatalogPointerMissing()
      throws Exception {
    // Snapshot 1 has unique files at root and in a nested directory.
    Files.writeString(fs.getPath("/home/snap1-root.txt"), "snap1-root-unique-payload");
    Files.writeString(fs.getPath("/home/sub/snap1-nested.txt"), "snap1-nested-unique-payload");
    String snap1CatalogSig = takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));
    Set<String> snap1Meta = storage.getFileNames(CloudFileStorage.ObjectType.METADATA);
    Set<String> snap1Data = storage.getFileNames(CloudFileStorage.ObjectType.DATA);

    // Snapshot 2 deletes the snapshot-1-only files so /home and /home/sub get new directory blocks
    // with previous_version pointing back to snapshot 1's directory blocks.
    Files.delete(fs.getPath("/home/snap1-root.txt"));
    Files.delete(fs.getPath("/home/sub/snap1-nested.txt"));
    Files.writeString(fs.getPath("/home/snap2-only.txt"), "snap2-unique-payload");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    // Delete snapshot 1's backups/ pointer and catalog block so snapshot 1's directories and
    // data blocks are reachable ONLY via Directory.previous_version links from snapshot 2.
    String snap1Pointer =
        snap1Meta.stream().filter(n -> n.startsWith("backups/")).findFirst().orElseThrow();
    String snap1CatalogBlock =
        snap1Meta.stream()
            .filter(n -> n.contains("/" + snap1CatalogSig + ":"))
            .findFirst()
            .orElseThrow();
    storage.delete(snap1Pointer);
    storage.delete(snap1CatalogBlock);

    Set<String> remainingSnap1Dirs =
        snap1Meta.stream()
            .filter(n -> !n.equals(snap1Pointer) && !n.equals(snap1CatalogBlock))
            .collect(Collectors.toSet());
    assertThat(remainingSnap1Dirs).hasSize(3); // /, /home, and /home/sub from snapshot 1

    // Under default / --snapshots all, previous_version links keep all snapshot 1 directories
    // and data blocks reachable (not orphaned), classify them as metadata, and exclude them from
    // --type data.
    assertThat(runListObjects("-c", "/config.properties", "--type", "orphaned")).isEmpty();
    assertThat(
            runListObjects("-c", "/config.properties", "--type", "orphaned", "--snapshots", "all"))
        .isEmpty();
    assertThat(runListObjects("-c", "/config.properties", "--type", "metadata"))
        .containsAtLeastElementsIn(remainingSnap1Dirs);
    assertThat(runListObjects("-c", "/config.properties", "--type", "data"))
        .containsAtLeastElementsIn(snap1Data);
    assertThat(runListObjects("-c", "/config.properties", "--type", "data"))
        .containsNoneIn(remainingSnap1Dirs);

    // With --snapshots 1, parent links (base_catalog and previous_version) are NOT followed.
    List<String> snap1ScopedOrphans =
        runListObjects("-c", "/config.properties", "--type", "orphaned", "--snapshots", "1");
    assertThat(snap1ScopedOrphans).containsAtLeastElementsIn(remainingSnap1Dirs);
    assertThat(runListObjects("-c", "/config.properties", "--type", "metadata", "--snapshots", "1"))
        .containsNoneIn(remainingSnap1Dirs);

    // Deleting a previous_version target (dangling parent link) does not fail list_objects and
    // omits the missing block from --type metadata.
    String deletedPrevDir = remainingSnap1Dirs.iterator().next();
    storage.delete(deletedPrevDir);
    assertThat(runListObjects("-c", "/config.properties", "--type", "metadata"))
        .doesNotContain(deletedPrevDir);
    runListObjects("-c", "/config.properties", "--type", "orphaned");
  }

  @Test
  public void maxListingAgeReusesPersistentCacheAcrossInvocations() throws Exception {
    Path cacheDir = tempFolder.newFolder("cache").toPath();
    Path configPath = tempFolder.newFile("config.properties").toPath();
    Path rootDir = tempFolder.newFolder("home").toPath();
    Files.writeString(rootDir.resolve("one.txt"), "first-file-payload");
    Files.writeString(
        configPath,
        "version: 1\n"
            + "max_split_size: 65536\n"
            + "disable_cache: false\n"
            + "cache_dir: "
            + cacheDir.toAbsolutePath()
            + "\n"
            + "include:\n"
            + "  - "
            + rootDir.toAbsolutePath()
            + "\n");

    AtomicInteger listCalls = new AtomicInteger();
    CloudFileStorage countingStorage =
        new CloudFileStorage() {
          @Override
          public ListenableFuture<?> store(
              String name,
              byte[] data,
              HashCode md5,
              Map<String, String> metadata,
              StoreOptions options) {
            return storage.store(name, data, md5, metadata, options);
          }

          @Override
          public Iterator<FileHeader> listFiles(String prefix, int limit) {
            listCalls.incrementAndGet();
            return storage.listFiles(prefix, limit);
          }

          @Override
          public ListenableFuture<Boolean> exists(String name) {
            return storage.exists(name);
          }

          @Override
          public ListenableFuture<byte[]> get(String name) {
            return storage.get(name);
          }

          @Override
          public void close() {
            storage.close();
          }
        };

    Instant t0 = Instant.parse("2026-06-01T00:00:00Z");
    TestCommandContext backupCtx = new TestCommandContext(FileSystems.getDefault(), countingStorage);
    backupCtx.clock = Clock.fixed(t0, ZoneOffset.UTC);
    new GletscherMain(backupCtx).run("backup", "-c", configPath.toString());
    assertThat(backupCtx.status).isEqualTo(0);

    // First list_objects (live) scans GCS and warms cache.mv.
    TestCommandContext liveCtx = new TestCommandContext(FileSystems.getDefault(), countingStorage);
    liveCtx.clock = Clock.fixed(t0, ZoneOffset.UTC);
    new GletscherMain(liveCtx).run("list_objects", "-c", configPath.toString(), "--type", "data");
    assertThat(liveCtx.status).isEqualTo(0);
    int callsAfterLive = listCalls.get();
    assertThat(callsAfterLive).isGreaterThan(0);

    // Second list_objects with --max-listing-age 1h at t0 + 30m makes 0 live list calls.
    TestCommandContext cachedCtx = new TestCommandContext(FileSystems.getDefault(), countingStorage);
    cachedCtx.clock = Clock.fixed(t0.plusSeconds(1800), ZoneOffset.UTC);
    new GletscherMain(cachedCtx)
        .run(
            "list_objects",
            "-c",
            configPath.toString(),
            "--type",
            "data",
            "--max-listing-age",
            "1h");
    assertThat(cachedCtx.status).isEqualTo(0);
    assertThat(cachedCtx.stdOutString()).isEqualTo(liveCtx.stdOutString());
    assertThat(listCalls.get()).isEqualTo(callsAfterLive);

    // Past max-listing-age (t0 + 30m > 15m), it refreshes from GCS cleanly.
    TestCommandContext expiredCtx =
        new TestCommandContext(FileSystems.getDefault(), countingStorage);
    expiredCtx.clock = Clock.fixed(t0.plusSeconds(1800), ZoneOffset.UTC);
    new GletscherMain(expiredCtx)
        .run(
            "list_objects",
            "-c",
            configPath.toString(),
            "--type",
            "data",
            "--max-listing-age",
            "15m");
    assertThat(expiredCtx.status).isEqualTo(0);
    assertThat(listCalls.get()).isGreaterThan(callsAfterLive);
  }

  @Test
  public void invalidUsageReturnsError() throws Exception {
    assertInvalidUsage(
        "Invalid --type value",
        "-c",
        "/config.properties",
        "--type",
        "invalid");
    assertInvalidUsage(
        "--snapshots can only be used with --type metadata or --type orphaned",
        "-c",
        "/config.properties",
        "--type",
        "data",
        "--snapshots",
        "2");
    assertInvalidUsage(
        "--snapshots can only be used with --type metadata or --type orphaned",
        "-c",
        "/config.properties",
        "--snapshots",
        "all");
    assertInvalidUsage(
        "--snapshots must be at least 1 or 'all'",
        "-c",
        "/config.properties",
        "--type",
        "metadata",
        "--snapshots",
        "0");
    assertInvalidUsage(
        "--snapshots must be at least 1 or 'all'",
        "-c",
        "/config.properties",
        "--type",
        "metadata",
        "--snapshots",
        "-1");
    assertInvalidUsage(
        "Invalid --snapshots value",
        "-c",
        "/config.properties",
        "--type",
        "orphaned",
        "--snapshots",
        "abc");
    assertInvalidUsage(
        "Invalid --min-size",
        "-c",
        "/config.properties",
        "--min-size",
        "not-a-size");
    assertInvalidUsage(
        "Invalid --min-age",
        "-c",
        "/config.properties",
        "--min-age",
        "soon");
    assertInvalidUsage(
        "Invalid --max-listing-age duration",
        "-c",
        "/config.properties",
        "--max-listing-age",
        "soon");
    assertInvalidUsage(
        "Invalid --max-listing-age duration",
        "-c",
        "/config.properties",
        "--max-listing-age",
        "-1h");
    assertInvalidUsage(
        "--max-listing-age must be non-negative",
        "-c",
        "/config.properties",
        "--max-listing-age",
        "-PT1H");
    assertInvalidUsage(
        "Command does not accept positional arguments",
        "-c",
        "/config.properties",
        "extra");
  }

  private String takeBackupAt(Instant when) throws Exception {
    TestCommandContext backupContext = new TestCommandContext(fs, storage);
    backupContext.clock = Clock.fixed(when, ZoneOffset.UTC);
    GletscherMain main = new GletscherMain(backupContext);
    main.run("backup", "-c", "/config.properties");
    assertThat(backupContext.status).isEqualTo(0);
    return Arrays.stream(backupContext.stdOutString().split("\\R"))
        .filter(line -> line.startsWith("new catalog: "))
        .map(line -> line.substring("new catalog: ".length()).trim())
        .findFirst()
        .orElseThrow();
  }

  private List<String> runListObjects(String... args) throws Exception {
    return runListObjectsAt(Instant.parse("2026-06-01T00:00:00Z"), args);
  }

  private record ListingResult(List<String> lines, String stdErr) {}

  private List<String> runListObjectsAt(Instant now, String... args) throws Exception {
    ListingResult result = runListObjectsDetailed(now, args);
    if (result.lines().isEmpty()) {
      assertThat(result.stdErr()).isEmpty();
    }
    return result.lines();
  }

  private ListingResult runListObjectsDetailed(Instant now, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, storage);
    context.clock = Clock.fixed(now, ZoneOffset.UTC);
    GletscherMain main = new GletscherMain(context);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "list_objects";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    main.run(fullArgs);
    assertThat(context.status).isEqualTo(0);
    String out = context.stdOutString();
    List<String> lines = out.isEmpty() ? List.of() : Arrays.asList(out.split("\\R"));
    return new ListingResult(lines, context.stdErrString());
  }

  private void assertInvalidUsage(String expectedMessage, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, storage);
    GletscherMain main = new GletscherMain(context);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "list_objects";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    main.run(fullArgs);
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
    assertThat(context.stdErrString()).contains(expectedMessage);
  }
}

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

import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CompressingCloudFileStorage;
import ws.moor.gletscher.cloud.EncryptingCloudFileStorage;
import ws.moor.gletscher.cloud.SigningCloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.util.Compressor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class VerifyCommandTest {

  private TestCommandContext context;
  private GletscherMain main;
  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem(unix());
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
    Files.newBufferedWriter(fs.getPath("/home/file.txt")).append("Hello World").close();

    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);
  }

  @Test
  public void testSuccessfulVerify() throws Exception {
    takeBackup();

    main.run("verify", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString())
        .contains("Discovered 5 object(s) in GCS (1 snapshot(s), 4 block(s)).");
    assertThat(context.stdOutString())
        .contains("Verified 1 unique catalog(s) and 2 unique directories; checked 1 unique data block(s).");
    assertThat(context.stdOutString()).contains("Catalog is complete.");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testFailedVerify() throws Exception {
    takeBackup();
    inMemoryStorage.delete(
        "blocks/50/08/50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11");

    main.run("verify", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .contains("Discovered 4 object(s) in GCS (1 snapshot(s), 3 block(s)).");
    assertThat(context.stdOutString())
        .contains("Verified 1 unique catalog(s) and 2 unique directories; checked 1 unique data block(s).");
    assertThat(context.stdOutString())
        .contains(
            "missing block: 50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11 in /home/file.txt");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testVerifyStatsDeduplicateSharedDirectoriesAndDataBlocks() throws Exception {
    Files.createDirectories(fs.getPath("/home/unchanged"));
    Files.writeString(fs.getPath("/home/unchanged/a.txt"), "shared");
    Files.writeString(fs.getPath("/home/unchanged/b.txt"), "shared");
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));

    Files.writeString(fs.getPath("/home/second.txt"), "second");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    // Snapshot 1: catalog1 + /, /home, /home/unchanged (4 meta) + "Hello World", "shared" (2 data).
    // Snapshot 2: catalog2 + /, /home (3 new meta; reuses /home/unchanged) + "second" (1 new data).
    // Total in GCS: 2 snapshots + 10 blocks = 12 objects.
    runVerify("-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString())
        .contains("Discovered 12 object(s) in GCS (2 snapshot(s), 10 block(s)).");
    assertThat(context.stdOutString())
        .contains(
            "Verified 2 unique catalog(s) and 5 unique directories; checked 3 unique data block(s).");
    assertThat(context.stdOutString()).contains("Verified 2 snapshot(s): Catalog is complete.");

    runVerify("-c", "/config.properties", "--snapshots", "1");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString())
        .contains("Discovered 11 object(s) in GCS (1 snapshot(s), 10 block(s)).");
    assertThat(context.stdOutString())
        .contains(
            "Verified 1 unique catalog(s) and 3 unique directories; checked 3 unique data block(s).");
    assertThat(context.stdOutString()).contains("Verified 1 snapshot(s): Catalog is complete.");
  }

  @Test
  public void testVerifyMultipleSnapshotsAndBrokenParentLink() throws Exception {
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));
    String snap1CatalogBlock =
        inMemoryStorage.getFileNames(ws.moor.gletscher.cloud.CloudFileStorage.ObjectType.METADATA)
            .stream()
            .filter(name -> name.startsWith("blocks/") && name.endsWith(":95"))
            .findFirst()
            .orElse(null);

    Files.writeString(fs.getPath("/home/second.txt"), "second snapshot data");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    // All snapshots and parent links valid.
    runVerify("-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Verified 2 snapshot(s): Catalog is complete.");

    // Delete the first snapshot's data block and catalog block; --snapshots all reports both errors
    // without aborting on the first failure, whereas --snapshots 1 still passes for the latest snapshot.
    inMemoryStorage.delete(
        "blocks/50/08/50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11");
    if (snap1CatalogBlock != null) {
      inMemoryStorage.delete(snap1CatalogBlock);
    }

    // Wait: /home/file.txt is shared by snapshot 2 too, so recreate/modify file.txt? No, verify with
    // snap1CatalogBlock or missing directory/file block works cleanly! Let's check --snapshots all:
    runVerify("-c", "/config.properties", "--snapshots", "all");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .contains(
            "missing block: 50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11 in /home/file.txt");
  }

  @Test
  public void testVerifyReportsBrokenOldSnapshotAndParentDirectoryLink() throws Exception {
    // Snapshot 1 has a unique file; snapshot 2 deletes it and adds a new file.
    Files.writeString(fs.getPath("/home/old-only.txt"), "unique-old-only-content");
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));
    Set<String> snap1Meta =
        inMemoryStorage.getFileNames(ws.moor.gletscher.cloud.CloudFileStorage.ObjectType.METADATA);
    Set<String> snap1Data =
        inMemoryStorage.getFileNames(ws.moor.gletscher.cloud.CloudFileStorage.ObjectType.DATA);

    Files.delete(fs.getPath("/home/old-only.txt"));
    Files.writeString(fs.getPath("/home/new.txt"), "brand-new-content");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));
    Set<String> allMetaAfterSnap2 =
        inMemoryStorage.getFileNames(ws.moor.gletscher.cloud.CloudFileStorage.ObjectType.METADATA);

    // Delete snapshot 1's directory/catalog blocks (everything unique to snapshot 1 except its pointer).
    String snap1Pointer =
        snap1Meta.stream().filter(n -> n.startsWith("backups/")).findFirst().orElseThrow();
    for (String block : snap1Meta) {
      if (!block.equals(snap1Pointer)) {
        inMemoryStorage.delete(block);
      }
    }
    // Also remove unique old data block.
    for (String dataBlock : snap1Data) {
      if (!inMemoryStorage
          .getFileNames(ws.moor.gletscher.cloud.CloudFileStorage.ObjectType.DATA)
          .isEmpty()
          && !dataBlock.endsWith(":11")) {
        inMemoryStorage.delete(dataBlock);
      }
    }

    // --snapshots 1 only checks snapshot 2 (whose own blocks are all intact) and succeeds!
    assertThat(allMetaAfterSnap2.size()).isGreaterThan(snap1Meta.size());
    runVerify("-c", "/config.properties", "--snapshots", "1");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Verified 1 snapshot(s): Catalog is complete.");

    // Default (--snapshots all) catches BOTH the broken snapshot 1 AND snapshot 2's broken
    // base_catalog / previous_version parent links!
    runVerify("-c", "/config.properties");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("broken snapshot " + snap1Pointer);
    assertThat(context.stdOutString()).contains("missing base_catalog block:");
    assertThat(context.stdOutString()).contains("missing previous_version directory block:");
  }

  @Test
  public void testInvalidSnapshotsUsage() throws Exception {
    runVerify("-c", "/config.properties", "--snapshots", "0");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    runVerify("-c", "/config.properties", "--snapshots", "-1");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    runVerify("-c", "/config.properties", "--snapshots", "bogus");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("Invalid --snapshots value");
  }

  @Test
  public void testPretendMissingOrphanedBlocksPassesWithoutModifyingStorage() throws Exception {
    takeBackup();

    // Add an unreferenced/aborted block.
    Signer signer =
        new Signer(Configuration.fromFile(fs.getPath("/config.properties")).getSigningKey());
    BlockStore blockStore =
        new BlockStore(new SigningCloudFileStorage(inMemoryStorage, signer), signer);
    byte[] orphanPayload = "aborted-upload-data".getBytes(StandardCharsets.UTF_8);
    String orphanBlock =
        BlockStore.toFileName(
            blockStore.store(orphanPayload, false, CloudFileStorage.ObjectType.DATA).get());
    int countBefore = inMemoryStorage.getFileCount();

    Files.writeString(
        fs.getPath("/candidates.txt"),
        "# deletion candidates\n\n  " + orphanBlock + "  \n");

    runVerify("-c", "/config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Pretending 1 object(s) do not exist.");
    assertThat(context.stdOutString()).contains("Verified 1 snapshot(s): Catalog is complete.");
    assertThat(context.stdErrString()).isEmpty();
    assertThat(inMemoryStorage.getFileCount()).isEqualTo(countBefore);
    assertThat(inMemoryStorage.exists(orphanBlock).get()).isTrue();
  }

  @Test
  public void testPretendMissingReferencedBlocksFails() throws Exception {
    takeBackup();
    String dataBlock =
        "blocks/50/08/50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11";
    Files.writeString(fs.getPath("/candidates.txt"), dataBlock + "\n");

    runVerify("-c", "/config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .contains(
            "missing block: 50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11 in /home/file.txt");
    assertThat(inMemoryStorage.exists(dataBlock).get()).isTrue();

    // Also test pretending the snapshot's directory block, catalog block, and pointer are missing.
    Configuration cfg = Configuration.fromFile(fs.getPath("/config.properties"));
    Signer signer = new Signer(cfg.getSigningKey());
    CloudFileStorage stacked =
        new CompressingCloudFileStorage(
            new EncryptingCloudFileStorage(
                new SigningCloudFileStorage(inMemoryStorage, signer),
                new Cryptor(cfg.getEncryptionKey(), cfg.getSigningKey())),
            new Compressor());
    BlockStore blockStore = new BlockStore(stacked, signer);
    CatalogStore catalogStore = new CatalogStore(fs, stacked, blockStore);
    CatalogStore.Snapshot snapshot =
        catalogStore.loadSnapshot(catalogStore.listSnapshotHeaders(1).get(0));

    String dirBlock = BlockStore.toFileName(snapshot.catalog().getOnlyRootBlock());
    Files.writeString(fs.getPath("/candidates.txt"), dirBlock + "\n");
    runVerify("-c", "/config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("missing directory block:");

    String catalogBlock = BlockStore.toFileName(snapshot.catalogBlock().orElseThrow());
    Files.writeString(fs.getPath("/candidates.txt"), catalogBlock + "\n");
    runVerify("-c", "/config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("broken snapshot " + snapshot.pointerName());

    Files.writeString(fs.getPath("/candidates.txt"), snapshot.pointerName() + "\n");
    runVerify("-c", "/config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("No existing backups found");
  }

  @Test
  public void testPretendMissingWithListObjectsOrphanedWorkflow() throws Exception {
    Files.write(
        fs.getPath("/gcs-config.properties"),
        ("version: 1\n"
                + "max_split_size: 65536\n"
                + "disable_cache: true\n"
                + "gcs:\n"
                + "  bucket_name: my-backup-bucket\n"
                + "  object_prefix: prod/\n"
                + "include:\n"
                + "  - /home\n")
            .getBytes(StandardCharsets.UTF_8));

    Files.writeString(fs.getPath("/home/old-only.txt"), "unique-old-only-content");
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"), "/gcs-config.properties");

    Files.delete(fs.getPath("/home/old-only.txt"));
    Files.writeString(fs.getPath("/home/new.txt"), "brand-new-content");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"), "/gcs-config.properties");

    // Generate deletion candidates as gs:// URIs for pruning down to the last 1 snapshot.
    TestCommandContext listContext = new TestCommandContext(fs, inMemoryStorage);
    new GletscherMain(listContext)
        .run(
            "list_objects",
            "-c",
            "/gcs-config.properties",
            "--type",
            "orphaned",
            "--snapshots",
            "1",
            "--gs-uri");
    assertThat(listContext.status).isEqualTo(0);
    String candidates = listContext.stdOutString();
    assertThat(candidates).contains("gs://my-backup-bucket/prod/backups/");
    Files.writeString(fs.getPath("/candidates.txt"), candidates);

    // Verifying --snapshots 1 with those candidates pretended missing succeeds.
    runVerify(
        "-c",
        "/gcs-config.properties",
        "--snapshots",
        "1",
        "--pretend-missing",
        "/candidates.txt");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Verified 1 snapshot(s): Catalog is complete.");

    // Verifying --snapshots all catches the broken parent links to snapshot 1!
    runVerify("-c", "/gcs-config.properties", "--pretend-missing", "/candidates.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("missing base_catalog block:");
    assertThat(context.stdOutString()).contains("missing previous_version directory block:");
  }

  @Test
  public void testPretendMissingMultipleFilesAndWithLocalCacheEnabled() throws Exception {
    Files.createDirectories(fs.getPath("/cache"));
    Files.write(
        fs.getPath("/cached-config.properties"),
        ("version: 1\n"
                + "max_split_size: 65536\n"
                + "disable_cache: false\n"
                + "cache_dir: /cache\n"
                + "include:\n"
                + "  - /home\n")
            .getBytes(StandardCharsets.UTF_8));

    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"), "/cached-config.properties");
    // Prime cache with a verify pass.
    runVerify("-c", "/cached-config.properties");
    assertThat(context.status).isEqualTo(0);

    String dataBlock =
        "blocks/50/08/50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11";
    Files.writeString(fs.getPath("/part2.txt"), dataBlock + "\n");
    runVerify("-c", "/cached-config.properties", "--pretend-missing", "/part2.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("Pretending 1 object(s) do not exist.");
    assertThat(context.stdOutString()).contains("missing block:");

    // Empty file and multiple files work cleanly.
    Files.writeString(fs.getPath("/empty.txt"), "\n# comment only\n");
    runVerify(
        "-c",
        "/cached-config.properties",
        "--pretend-missing",
        "/empty.txt");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Pretending 0 object(s) do not exist.");

    runVerify(
        "-c",
        "/cached-config.properties",
        "--pretend-missing",
        "/empty.txt",
        "--pretend-missing",
        "/part2.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains("Pretending 1 object(s) do not exist.");
  }

  @Test
  public void testPretendMissingInvalidUsage() throws Exception {
    takeBackup();

    // Missing file.
    runVerify("-c", "/config.properties", "--pretend-missing", "/does-not-exist.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("File not found for --pretend-missing");

    // Directory instead of file.
    runVerify("-c", "/config.properties", "--pretend-missing", "/home");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("--pretend-missing path is a directory");

    // gs:// URI without gcs.bucket_name in config.
    Files.writeString(fs.getPath("/gs.txt"), "gs://some-bucket/blocks/00/00/aa:1\n");
    runVerify("-c", "/config.properties", "--pretend-missing", "/gs.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("gcs.bucket_name is required");

    // gs:// URI with mismatched bucket or prefix.
    Files.write(
        fs.getPath("/gcs-config.properties"),
        ("version: 1\n"
                + "max_split_size: 65536\n"
                + "disable_cache: true\n"
                + "gcs:\n"
                + "  bucket_name: my-backup-bucket\n"
                + "  object_prefix: prod/\n"
                + "include:\n"
                + "  - /home\n")
            .getBytes(StandardCharsets.UTF_8));
    Files.writeString(
        fs.getPath("/wrong-prefix.txt"),
        "gs://my-backup-bucket/staging/blocks/00/00/aa:1\n");
    runVerify("-c", "/gcs-config.properties", "--pretend-missing", "/wrong-prefix.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString())
        .contains("expected prefix 'gs://my-backup-bucket/prod/'");

    // gs:// URI with empty relative name.
    Files.writeString(fs.getPath("/empty-rel.txt"), "gs://my-backup-bucket/prod/\n");
    runVerify("-c", "/gcs-config.properties", "--pretend-missing", "/empty-rel.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString())
        .contains("expected prefix 'gs://my-backup-bucket/prod/'");

    // Non-blocks/backups entries emit a warning on stderr.
    Files.writeString(fs.getPath("/unexpected-entry.txt"), "unexpected/object/name\n");
    runVerify("-c", "/config.properties", "--pretend-missing", "/unexpected-entry.txt");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString())
        .contains(
            "Warning: --pretend-missing entry 'unexpected/object/name' at /unexpected-entry.txt:1 "
                + "does not start with 'blocks/' or 'backups/'.");
  }

  private void runVerify(String... args) throws Exception {
    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "verify";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    main.run(fullArgs);
  }

  private void takeBackup() throws Exception {
    takeBackupAt(Instant.now());
  }

  private void takeBackupAt(Instant when) throws Exception {
    takeBackupAt(when, "/config.properties");
  }

  private void takeBackupAt(Instant when, String configPath) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    context.clock = Clock.fixed(when, ZoneOffset.UTC);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", configPath);
    assertThat(context.status).isEqualTo(0);
  }
}

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

import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.FileReadFailureInjector;
import ws.moor.gletscher.commands.testing.TestCommandContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;
import static java.lang.System.lineSeparator;

@RunWith(JUnit4.class)
public class ContainsCommandTest {

  private static final ZoneId ZURICH = ZoneId.of("Europe/Zurich");

  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem(unix());
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    Files.writeString(
        fs.getPath("/config.properties"),
        """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """,
        StandardCharsets.UTF_8);

    Files.createDirectories(fs.getPath("/home/sub"));
    Files.createDirectories(fs.getPath("/check"));
  }

  @Test
  public void testAllFoundInLatestSnapshotWithMultiplePathsAndLocalTimestamp() throws Exception {
    Files.writeString(fs.getPath("/home/a.txt"), "alpha-content");
    Files.writeString(fs.getPath("/home/sub/a_copy.txt"), "alpha-content");
    Files.writeString(fs.getPath("/home/b.txt"), "beta-content");
    takeBackupAt(Instant.parse("2026-01-15T10:00:00Z"));

    Files.writeString(fs.getPath("/check/first.bin"), "beta-content");
    Files.writeString(fs.getPath("/check/second.bin"), "alpha-content");
    Files.writeString(fs.getPath("/check/third.bin"), "alpha-content");

    TestCommandContext context =
        runContains(
            ZURICH,
            "-c",
            "/config.properties",
            "/check/first.bin",
            "/check/second.bin",
            "/check/third.bin");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 3 files (38 B).",
                    "Searching snapshot 2026-01-15 11:00:00 CET...",
                    "",
                    "Found:",
                    "  /check/first.bin (in snapshot 2026-01-15 11:00:00 CET):",
                    "    /home/b.txt",
                    "  /check/second.bin (in snapshot 2026-01-15 11:00:00 CET):",
                    "    /home/a.txt",
                    "    /home/sub/a_copy.txt",
                    "  /check/third.bin (in snapshot 2026-01-15 11:00:00 CET):",
                    "    /home/a.txt",
                    "    /home/sub/a_copy.txt")
                + lineSeparator());
  }

  @Test
  public void testMixedEmptyFoundAndNotFoundPreservesOrderWithinSections() throws Exception {
    Files.writeString(fs.getPath("/home/kept.txt"), "kept-data");
    Files.writeString(fs.getPath("/home/backup-empty.txt"), "");
    takeBackupAt(Instant.parse("2026-07-01T12:00:00Z"));

    Files.writeString(fs.getPath("/check/missing1.txt"), "nope-1");
    Files.writeString(fs.getPath("/check/empty1.txt"), "");
    Files.writeString(fs.getPath("/check/found1.txt"), "kept-data");
    Files.writeString(fs.getPath("/check/empty2.txt"), "");
    Files.writeString(fs.getPath("/check/missing2.txt"), "nope-2");

    TestCommandContext context =
        runContains(
            ZURICH,
            "-c",
            "/config.properties",
            "/check/missing1.txt",
            "/check/empty1.txt",
            "/check/found1.txt",
            "/check/empty2.txt",
            "/check/missing2.txt");

    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 5 files (21 B).",
                    "Searching snapshot 2026-07-01 14:00:00 CEST...",
                    "",
                    "Empty (0 bytes):",
                    "  /check/empty1.txt",
                    "  /check/empty2.txt",
                    "",
                    "Found:",
                    "  /check/found1.txt (in snapshot 2026-07-01 14:00:00 CEST):",
                    "    /home/kept.txt",
                    "",
                    "Not found:",
                    "  /check/missing1.txt",
                    "  /check/missing2.txt")
                + lineSeparator());
  }

  @Test
  public void testOnlyEmptyFilesSucceedsWithoutBackups() throws Exception {
    Files.writeString(fs.getPath("/check/empty.txt"), "");

    TestCommandContext context = runContains(ZURICH, "-c", "/config.properties", "/check/empty.txt");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 1 file (0 B).",
                    "",
                    "Empty (0 bytes):",
                    "  /check/empty.txt")
                + lineSeparator());
  }

  @Test
  public void testSnapshotLimitDefaultsToLatestAndSupportsOlderWithN() throws Exception {
    // Snapshot 1: old-only + version 1 of shared + unchanged
    Files.writeString(fs.getPath("/home/old.txt"), "old-only-content");
    Files.writeString(fs.getPath("/home/shared.txt"), "shared-v1");
    Files.writeString(fs.getPath("/home/unchanged.txt"), "unchanged-content");
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));

    // Snapshot 2: old.txt removed, shared.txt updated to v2, unchanged.txt still present
    Files.delete(fs.getPath("/home/old.txt"));
    Files.writeString(fs.getPath("/home/shared.txt"), "shared-v2");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    Files.writeString(fs.getPath("/check/old.txt"), "old-only-content");
    Files.writeString(fs.getPath("/check/shared_v1.txt"), "shared-v1");
    Files.writeString(fs.getPath("/check/unchanged.txt"), "unchanged-content");

    // Default (-n 1): only latest snapshot is searched.
    TestCommandContext latestOnly =
        runContains(
            ZoneOffset.UTC,
            "-c",
            "/config.properties",
            "/check/old.txt",
            "/check/shared_v1.txt",
            "/check/unchanged.txt");
    assertThat(latestOnly.status).isEqualTo(-1);
    assertThat(latestOnly.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 3 files (42 B).",
                    "Searching snapshot 2026-01-01 00:00:00 Z...",
                    "",
                    "Found:",
                    "  /check/unchanged.txt (in snapshot 2026-01-01 00:00:00 Z):",
                    "    /home/unchanged.txt",
                    "",
                    "Not found:",
                    "  /check/old.txt",
                    "  /check/shared_v1.txt")
                + lineSeparator());

    // -n 2: searches both, reports unchanged.txt only in snapshot 2 (latest), and others in snapshot 1.
    TestCommandContext bothSnapshots =
        runContains(
            ZoneOffset.UTC,
            "-c",
            "/config.properties",
            "-n",
            "2",
            "/check/old.txt",
            "/check/shared_v1.txt",
            "/check/unchanged.txt");
    assertThat(bothSnapshots.status).isEqualTo(0);
    assertThat(bothSnapshots.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 3 files (42 B).",
                    "Searching snapshot 2026-01-01 00:00:00 Z...",
                    "Searching snapshot 2025-01-01 00:00:00 Z...",
                    "",
                    "Found:",
                    "  /check/old.txt (in snapshot 2025-01-01 00:00:00 Z):",
                    "    /home/old.txt",
                    "  /check/shared_v1.txt (in snapshot 2025-01-01 00:00:00 Z):",
                    "    /home/shared.txt",
                    "  /check/unchanged.txt (in snapshot 2026-01-01 00:00:00 Z):",
                    "    /home/unchanged.txt")
                + lineSeparator());
  }

  @Test
  public void testStopsSearchingOlderSnapshotsOnceAllFilesAreFound() throws Exception {
    Files.writeString(fs.getPath("/home/old.txt"), "old-content");
    takeBackupAt(Instant.parse("2025-01-01T00:00:00Z"));
    List<String> snap1Meta =
        new ArrayList<>(inMemoryStorage.getFileNames(CloudFileStorage.ObjectType.METADATA));

    Files.writeString(fs.getPath("/home/latest.txt"), "latest-content");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    // Corrupt snapshot 1 so loading it would fail if touched.
    for (String meta : snap1Meta) {
      if (meta.startsWith("blocks/")) {
        inMemoryStorage.delete(meta);
      }
    }

    Files.writeString(fs.getPath("/check/latest.txt"), "latest-content");
    TestCommandContext context =
        runContains(
            ZoneOffset.UTC, "-c", "/config.properties", "--snapshots", "all", "/check/latest.txt");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("Searching snapshot 2026-01-01 00:00:00 Z...");
    assertThat(context.stdOutString()).doesNotContain("2025-01-01");
    assertThat(context.stdOutString()).contains("/home/latest.txt");
  }

  @Test
  public void testNoBackupsReturnsErrorForNonEmptyFile() throws Exception {
    Files.writeString(fs.getPath("/check/missing.txt"), "data");

    TestCommandContext context =
        runContains(ZoneOffset.UTC, "-c", "/config.properties", "/check/missing.txt");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .isEqualTo(
            String.join(
                    lineSeparator(),
                    "Fingerprinted 1 file (4 B).",
                    "",
                    "Not found:",
                    "  /check/missing.txt")
                + lineSeparator());
  }

  @Test
  public void testInvalidUsageAndBadInputs() throws Exception {
    TestCommandContext missingArgs = runContains(ZoneOffset.UTC, "-c", "/config.properties");
    assertThat(missingArgs.status).isEqualTo(-1);
    assertThat(missingArgs.stdErrString()).contains("Must provide at least one file name.");

    TestCommandContext invalidSnapshots =
        runContains(ZoneOffset.UTC, "-c", "/config.properties", "-n", "0", "/check/nonexistent");
    assertThat(invalidSnapshots.status).isEqualTo(-1);
    assertThat(invalidSnapshots.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    TestCommandContext dirArg = runContains(ZoneOffset.UTC, "-c", "/config.properties", "/home");
    assertThat(dirArg.status).isEqualTo(-1);
    assertThat(dirArg.stdErrString()).contains("not a regular file: /home");

    Files.writeString(fs.getPath("/check/unreadable.txt"), "secret");
    TestCommandContext readFailure = new TestCommandContext(fs, inMemoryStorage);
    readFailure.failureInjector = new FileReadFailureInjector.FailSpecificNamesInjector("unreadable.txt");
    new GletscherMain(readFailure)
        .run("contains", "-c", "/config.properties", "/check/unreadable.txt");
    assertThat(readFailure.status).isEqualTo(-1);
    assertThat(readFailure.stdErrString()).contains("failed to read /check/unreadable.txt");
  }

  private void takeBackupAt(Instant instant) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    context.clock = Clock.fixed(instant, ZoneOffset.UTC);
    new GletscherMain(context).run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }

  private TestCommandContext runContains(ZoneId zoneId, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    context.clock = Clock.fixed(Instant.parse("2026-11-25T12:00:00Z"), zoneId);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "contains";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    new GletscherMain(context).run(fullArgs);
    return context;
  }
}

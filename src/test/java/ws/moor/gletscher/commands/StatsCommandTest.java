/*
 * Copyright 2020 Patrick Moor <patrick@moor.ws>
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
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class StatsCommandTest {

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
    Files.newBufferedWriter(fs.getPath("/home/me-1.jpg")).append("xyz").close();
    Files.newBufferedWriter(fs.getPath("/home/me-2.jpg")).append("qwert").close();
    Files.newBufferedWriter(fs.getPath("/home/me-3.identical.gif")).append("qwert").close();
    Files.newBufferedWriter(fs.getPath("/home/no-extension")).append("0000").close();
    Files.newBufferedWriter(fs.getPath("/home/some.superlongextension")).append("99").close();

    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);
  }

  @Test
  public void testStats() throws Exception {
    takeBackup();

    main.run("stats", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("roots: 1");
    assertThat(context.stdOutString()).contains("directories: 2");
    assertThat(context.stdOutString()).contains("files: 6");
    assertThat(context.stdOutString()).contains("symlinks: 0");
    assertThat(context.stdOutString()).contains("meta blocks: 3");
    assertThat(context.stdOutString()).doesNotContain("unique meta blocks:");
    assertThat(context.stdOutString()).contains("total data blocks: 6");
    assertThat(context.stdOutString()).contains("unique data blocks: 5");
    assertThat(context.stdOutString()).contains("total data block size: 30 B");
    assertThat(context.stdOutString()).contains("unique data block size: 25 B");
    assertThat(context.stdOutString()).contains("txt: 11 B");
    assertThat(context.stdOutString()).contains("jpg: 8 B");
    assertThat(context.stdOutString()).contains("<none>: 6 B");
    assertThat(context.stdOutString()).contains("gif: 5 B");
    assertThat(context.stdOutString()).doesNotContain("File Blocks:");
    assertThat(context.stdOutString()).doesNotContain("Summary across");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testVerbose() throws Exception {
    takeBackup();

    main.run("stats", "-c", "/config.properties", "-v");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("File Blocks:");
    assertThat(context.stdOutString()).contains("total count: 6  total sum: 30 B");
    assertThat(context.stdOutString()).contains("Meta Blocks:");
    assertThat(context.stdOutString()).contains("total count: 3");
    assertThat(context.stdOutString()).contains("Original File Sizes:");
    assertThat(context.stdOutString()).contains("[ 2 B .. 10 B)");
    assertThat(context.stdOutString()).contains("[10 B .. 11 B]");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testSnapshotsOptionDefaultsToOneAndSupportsCountAndAll() throws Exception {
    Files.createDirectories(fs.getPath("/home/unchanged"));
    Files.writeString(fs.getPath("/home/unchanged/shared.txt"), "shared-data");
    takeBackupAt(Instant.parse("2026-01-01T00:00:00Z"));

    Files.writeString(fs.getPath("/home/added-in-2.txt"), "second");
    takeBackupAt(Instant.parse("2026-01-02T00:00:00Z"));

    Files.writeString(fs.getPath("/home/added-in-3.txt"), "third!");
    takeBackupAt(Instant.parse("2026-01-03T00:00:00Z"));

    // Default analyzes only the latest snapshot (1).
    runStats("-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(countOccurrences(context.stdOutString(), "Analyzing catalog ")).isEqualTo(1);
    assertThat(context.stdOutString()).contains("files: 9");
    assertThat(context.stdOutString()).doesNotContain("Summary across");
    assertThat(context.stdErrString()).isEmpty();

    // -n 2 analyzes the latest 2 snapshots and prints deduplicated summary across them.
    runStats("-c", "/config.properties", "-n", "2");
    assertThat(context.status).isEqualTo(0);
    assertThat(countOccurrences(context.stdOutString(), "Analyzing catalog ")).isEqualTo(2);
    assertThat(context.stdOutString()).contains("files: 9");
    assertThat(context.stdOutString()).contains("files: 8");
    assertThat(context.stdOutString()).doesNotContain("files: 7");
    assertThat(context.stdOutString()).contains("Summary across 2 snapshots:");
    // Each snapshot has 4 meta blocks (catalog, /, /home, /home/unchanged), but /home/unchanged is
    // shared across both snapshots so only 7 meta blocks are unique.
    assertThat(context.stdOutString()).contains("unique meta blocks: 7");
    assertThat(context.stdOutString()).contains("unique data blocks: 8");
    assertThat(context.stdErrString()).isEmpty();

    // --snapshots all analyzes all 3 snapshots.
    runStats("-c", "/config.properties", "--snapshots", "all");
    assertThat(context.status).isEqualTo(0);
    assertThat(countOccurrences(context.stdOutString(), "Analyzing catalog ")).isEqualTo(3);
    assertThat(context.stdOutString()).contains("files: 9");
    assertThat(context.stdOutString()).contains("files: 8");
    assertThat(context.stdOutString()).contains("files: 7");
    assertThat(context.stdOutString()).contains("Summary across 3 snapshots:");
    // /home/unchanged is shared across all 3 snapshots (1 + 3*3 = 10 unique meta blocks).
    assertThat(context.stdOutString()).contains("unique meta blocks: 10");
    assertThat(context.stdOutString()).contains("unique data blocks: 8");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testNoBackups() throws Exception {
    runStats("-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).isEmpty();
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testInvalidSnapshotsOption() throws Exception {
    runStats("-c", "/config.properties", "--snapshots", "0");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    runStats("-c", "/config.properties", "-n", "-1");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("--snapshots must be at least 1 or 'all'");

    runStats("-c", "/config.properties", "--snapshots", "bogus");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdErrString()).contains("Invalid --snapshots value");
  }

  private void runStats(String... args) throws Exception {
    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);
    String[] fullArgs = new String[args.length + 1];
    fullArgs[0] = "stats";
    System.arraycopy(args, 0, fullArgs, 1, args.length);
    main.run(fullArgs);
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }

  private void takeBackup() throws Exception {
    takeBackupAt(Instant.now());
  }

  private void takeBackupAt(Instant when) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    context.clock = Clock.fixed(when, ZoneOffset.UTC);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }
}

/*
 * Copyright 2019 Patrick Moor <patrick@moor.ws>
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
import ws.moor.gletscher.commands.testing.FileReadFailureInjector;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.testing.FileSystemTestRule;

import java.nio.file.FileSystem;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class BackupCommandTest {

  @Rule public FileSystemTestRule unix = FileSystemTestRule.unix();
  @Rule public FileSystemTestRule windows = FileSystemTestRule.windows();
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() {
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
  }

  @Test
  public void testNoFailure() throws Exception {
    unix.writeFile("/config.properties","""
            version: 1
            max_split_size: 65536
            disable_cache: true
            include:
              - /home
            """);
    unix.createDirectories("/home");
    unix.writeFile("/home/file.txt", "Hello World");
    unix.writeFile("/home/file2.txt", "Hello Two");

    TestCommandContext context = new TestCommandContext(unix.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("new file: /home/file.txt");
    assertThat(context.stdOutString()).contains("new file: /home/file2.txt");
    assertThat(context.stdErrString()).isEmpty();

    // 1 backup pointer + 1 catalog block + 2 directory blocks (/, /home) = 4 metadata objects
    assertThat(inMemoryStorage.getFileNames(CloudFileStorage.ObjectType.METADATA)).hasSize(4);
    // 2 file content blocks = 2 data objects
    assertThat(inMemoryStorage.getFileNames(CloudFileStorage.ObjectType.DATA)).hasSize(2);
  }

  @Test
  public void testFailure() throws Exception {
    unix.writeFile("/config.properties","""
            version: 1
            max_split_size: 65536
            disable_cache: true
            include:
              - /home
            """);
    unix.createDirectories("/home");
    unix.writeFile("/home/file.txt", "Hello World");
    unix.writeFile("/home/ioerror.txt", "");

    TestCommandContext context = new TestCommandContext(unix.getFileSystem(), inMemoryStorage);
    context.failureInjector = new FileReadFailureInjector.FailSpecificNamesInjector("ioerror.txt");

    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    // TODO: Should this command really still succeed?
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("new file: /home/file.txt");
    assertThat(context.stdOutString()).contains("new file: /home/ioerror.txt");
    assertThat(context.stdErrString()).contains("failed to read path: /home/ioerror.txt: "
        + "java.lang.RuntimeException: java.io.IOException: Injected Failure");
  }

  @Test
  public void testBackupWindowsFollowedByUnix() throws Exception {
    windows.writeFile("C:\\config.properties","""
            version: 1
            max_split_size: 65536
            disable_cache: true
            include:
              - C:\\Home
            """);
    windows.createDirectories("C:\\Home");
    windows.writeFile("C:\\Home\\file.txt", "Hello World");
    windows.writeFile("C:\\Home\\file2.txt", "Hello Two");
    windows.writeFile("C:\\Home\\file3.txt", "Hello Three");

    TestCommandContext context = new TestCommandContext(windows.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "C:\\config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(inMemoryStorage.getFileCount()).isEqualTo(7);

    // Now take a backup on unix with a home folder with matching contents.
    unix.writeFile("/config.properties","""
          version: 1
          max_split_size: 65536
          disable_cache: true
          include:
            - /home
          mappings:
            /home: C:\\Home
          """);
    windows.recursiveCopy("C:\\Home", unix.getFileSystem(), "/home");

    context = new TestCommandContext(unix.getFileSystem(), inMemoryStorage);
    main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);

    // Only 3 new files needed: new catalog reference, new catalog, new root directory. Everything else is re-used.
    assertThat(inMemoryStorage.getFileCount()).isEqualTo(10);
  }

  @Test
  public void testBackupNeverUsesCachedListingsOrAcceptsMaxListingAge() throws Exception {
    unix.createDirectories("/cache");
    unix.createDirectories("/home");
    unix.writeFile("/home/a.txt", "hello");
    unix.writeFile(
        "/cached-config.yaml",
        """
        version: 1
        max_split_size: 65536
        disable_cache: false
        cache_dir: /cache
        include:
          - /home
        """);

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
            return inMemoryStorage.store(name, data, md5, metadata, options);
          }

          @Override
          public Iterator<FileHeader> listFiles(String prefix, int limit) {
            listCalls.incrementAndGet();
            return inMemoryStorage.listFiles(prefix, limit);
          }

          @Override
          public ListenableFuture<Boolean> exists(String name) {
            return inMemoryStorage.exists(name);
          }

          @Override
          public ListenableFuture<byte[]> get(String name, GetOptions options) {
            return inMemoryStorage.get(name, options);
          }

          @Override
          public void close() {
            inMemoryStorage.close();
          }
        };

    FileSystem fs = unix.getFileSystem();

    // Rejects --max-listing-age on backup.
    TestCommandContext badUsage = new TestCommandContext(fs, countingStorage);
    new GletscherMain(badUsage)
        .run("backup", "-c", "/cached-config.yaml", "--max-listing-age", "1h");
    assertThat(badUsage.status).isEqualTo(-1);
    assertThat(badUsage.stdErrString()).contains("Unrecognized option: --max-listing-age");

    // First backup warms the backups/ listing cache; second backup must still list live.
    Instant t0 = Instant.parse("2026-06-01T00:00:00Z");
    TestCommandContext firstBackup = new TestCommandContext(fs, countingStorage);
    firstBackup.clock = Clock.fixed(t0, ZoneOffset.UTC);
    new GletscherMain(firstBackup).run("backup", "-c", "/cached-config.yaml");
    assertThat(firstBackup.status).isEqualTo(0);

    int callsBeforeSecondBackup = listCalls.get();
    TestCommandContext secondBackup = new TestCommandContext(fs, countingStorage);
    secondBackup.clock = Clock.fixed(t0.plusSeconds(60), ZoneOffset.UTC);
    new GletscherMain(secondBackup).run("backup", "-c", "/cached-config.yaml");
    assertThat(secondBackup.status).isEqualTo(0);
    assertThat(listCalls.get()).isEqualTo(callsBeforeSecondBackup + 1);
  }
}

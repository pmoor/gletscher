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

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.FileReadFailureInjector;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.testing.FileSystemTestRule;

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
}

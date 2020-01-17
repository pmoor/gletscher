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

import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.FileReadFailureInjector;
import ws.moor.gletscher.commands.testing.TestCommandContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class BackupCommandTest {

  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem();
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
    Files.newBufferedWriter(fs.getPath("/home/ioerror.txt")).append("Irrelevant").close();
  }

  @Test
  public void testNoFailure() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("new file: /home/file.txt");
    assertThat(context.stdOutString()).contains("new file: /home/ioerror.txt");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testFailure() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
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

}
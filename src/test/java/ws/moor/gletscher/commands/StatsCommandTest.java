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
    assertThat(context.stdOutString()).contains("total blocks: 6");
    assertThat(context.stdOutString()).contains("unique blocks: 5");
    assertThat(context.stdOutString()).contains("total block size: 30 B");
    assertThat(context.stdOutString()).contains("unique block size: 25 B");
    assertThat(context.stdOutString()).contains("txt: 11 B");
    assertThat(context.stdOutString()).contains("jpg: 8 B");
    assertThat(context.stdOutString()).contains("<none>: 6 B");
    assertThat(context.stdOutString()).contains("gif: 5 B");
    assertThat(context.stdErrString()).isEmpty();
  }

  private void takeBackup() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }
}

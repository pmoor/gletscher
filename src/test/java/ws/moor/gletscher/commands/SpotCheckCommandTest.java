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

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.hash.Hashing;
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

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class SpotCheckCommandTest {

  private TestCommandContext context;
  private GletscherMain main;
  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem();
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    Files.write(fs.getPath("/config.properties"),
        ("version: 1\n" +
            "max_split_size: 65536\n" +
            "disable_cache: true\n" +
            "include:\n" +
            "  - /home\n"
        ).getBytes(StandardCharsets.UTF_8));

    // files to back-up
    Files.createDirectories(fs.getPath("/home"));
    for (int i = 0; i < 20; i++) {
      String fileName = String.format("/home/file-%04d.txt", i);
      String contents = Strings.repeat(String.format("%04d", i), i * i * 1000);
      Files.newBufferedWriter(fs.getPath(fileName)).append(contents).close();
    }

    context = new TestCommandContext(fs, inMemoryStorage);
    main = new GletscherMain(context);

    SpotCheckCommand.hashFn = Hashing.sha256();
  }

  @Test
  public void testSuccessfulSpotCheck() throws Exception {
    takeBackup();

    main.run("spot_check", "-c", "/config.properties", "--max_blocks", "10");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString())
        .startsWith("checking /home/file-0017.txt (offset 131072, 65536 bytes): success.");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testFailedSpotCheck() throws Exception {
    takeBackup();

    int i = 17;
    String fileName = String.format("/home/file-%04d.txt", i);
    String contents = Strings.repeat(String.format("%04d", i * i), i * i * 1000);
    Files.newBufferedWriter(fs.getPath(fileName)).append(contents).close();

    main.run("spot_check", "-c", "/config.properties", "--max_blocks", "10");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString())
        .startsWith("checking /home/file-0017.txt (offset 131072, 65536 bytes): failed!");
    assertThat(context.stdErrString()).isEmpty();
  }

  private void takeBackup() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }
}

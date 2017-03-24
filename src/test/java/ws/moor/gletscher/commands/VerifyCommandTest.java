/*
 * Copyright 2017 Patrick Moor <patrick@moor.ws>
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

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class VerifyCommandTest {

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
            "split_algorithm: rolling\n" +
            "secret_key: !!binary AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n" +
            "include:\n" +
            "  - /home\n"
        ).getBytes(StandardCharsets.UTF_8));

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
    assertThat(context.stdOutString()).contains("Catalog is complete.");
    assertThat(context.stdErrString()).isEmpty();
  }

  @Test
  public void testFailedVerify() throws Exception {
    takeBackup();
    inMemoryStorage.delete("blocks/50/08/50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11");

    main.run("verify", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).contains(
        "missing block: 50082da69e7e4780c867be198e795b9cd5e94e739ee9485aa95a70f60e36e73f:11 in /home/file.txt");
    assertThat(context.stdErrString()).isEmpty();
  }

  private void takeBackup() throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }
}

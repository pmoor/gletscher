/*
 * Copyright 2016 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.proto.testing.Testing;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;


@RunWith(JUnit4.class)
public class BackwardsCompatibilityTest {

  private static final Instant TIME_A = Instant.ofEpochSecond(1187161200);
  private static final Instant TIME_B = Instant.ofEpochSecond(1313391600);
  private static final Instant TIME_C = Instant.ofEpochSecond(1356076800);

  private FileSystem fs;
  private InMemoryCloudFileStorage inMemoryStorage;

  @Before
  public void setUp() throws Exception {
    fs = Jimfs.newFileSystem(Configuration.unix());
    Files.write(fs.getPath("/config.properties"),
        ("version: 1\n" +
            "max_split_size: 10\n" +
            "disable_cache: true\n" +
            "split_algorithm: rolling\n" +
            "secret_key: !!binary AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n" +
            "include:\n" +
            "  - /home\n" +
            "exclude:\n" +
            "  - \\.ignore$"
        ).getBytes(StandardCharsets.UTF_8));

    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
  }


  @Test
  public void liveVersion() throws Exception {
    populateTestCase(fs);

    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "backup", "-c", "/config.properties");

    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "restore", "-c", "/config.properties", "/restore");
    assertProperlyRestored(fs.getPath("/restore"));

    // remove comment to create new test test
//    try (FileOutputStream fos = new FileOutputStream("/tmp/dump.bin")) {
//      inMemoryStorage.toProto().writeTo(fos);
//    }
  }

  @Test
  public void version20160930() throws Exception {
    inMemoryStorage.mergeFromProto(
        Testing.FileList.parseFrom(getClass().getResourceAsStream("/20160930.bin")));

    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "restore", "-c", "/config.properties", "/restore");
    assertProperlyRestored(fs.getPath("/restore"));
  }

  @Test
  public void version20160930newCryptor() throws Exception {
    inMemoryStorage.mergeFromProto(
        Testing.FileList.parseFrom(getClass().getResourceAsStream("/20160930-new-cryptor.bin")));

    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "restore", "-c", "/config.properties", "/restore");
    assertProperlyRestored(fs.getPath("/restore"));
  }

  private void runCommandAndAssertSuccess(FileSystem fs, CloudFileStorage inMemoryStorage, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain gletscherMain = new GletscherMain(context);
    gletscherMain.run(args);
    if (context.status != 0) {
      System.err.println("stdout:");
      System.err.println(context.stdOutString());
      System.err.println("stderr:");
      System.err.println(context.stdErrString());
      throw new IllegalStateException("command was not successful");
    }
  }

  private void populateTestCase(FileSystem fs) throws IOException {
    writeFile(fs.getPath("/home/userA/file-01.txt"), TIME_A, "Hello World!\n");
    writeFile(fs.getPath("/home/userA/file-02.txt"), TIME_B, "What is going on?\n");
    writeFile(fs.getPath("/home/userA/file-03.txt"), TIME_C, "Hello World!\n");

    writeFile(fs.getPath("/home/userB/file-01.txt"), TIME_A, "Hello World! How are we all doing?\n");
    writeFile(fs.getPath("/home/userB/file-02.txt"), TIME_B, "");

    Files.createSymbolicLink(fs.getPath("/home/userB/file-03.txt"), fs.getPath("file-02.txt"));
    Files.createSymbolicLink(fs.getPath("/home/userB/file-04.txt"), fs.getPath("/home/userA/file-01.txt"));
    Files.createSymbolicLink(fs.getPath("/home/userB/file-05.txt"), fs.getPath("/random-file-not-backed.up"));

    writeFile(fs.getPath("/home/file.ignore"), TIME_A, "ignored");
    writeFile(fs.getPath("/home/large-file.bin"), TIME_A, Strings.repeat("*", 512));
  }

  private void assertProperlyRestored(Path root) {
    assertThat(Files.isDirectory(root.resolve("home"))).isTrue();
    assertThat(Files.isDirectory(root.resolve("home/userA"))).isTrue();
    assertThat(Files.isRegularFile(root.resolve("home/userA/file-01.txt"))).isTrue();
    assertThat(Files.isRegularFile(root.resolve("home/userA/file-02.txt"))).isTrue();
    assertThat(Files.isRegularFile(root.resolve("home/userA/file-03.txt"))).isTrue();

    assertThat(Files.isDirectory(root.resolve("home/userB"))).isTrue();
    assertThat(Files.isRegularFile(root.resolve("home/userB/file-01.txt"))).isTrue();
    assertThat(Files.isRegularFile(root.resolve("home/userB/file-02.txt"))).isTrue();
    assertThat(Files.isSymbolicLink(root.resolve("home/userB/file-03.txt"))).isTrue();
    assertThat(Files.isSymbolicLink(root.resolve("home/userB/file-04.txt"))).isTrue();
    assertThat(Files.isSymbolicLink(root.resolve("home/userB/file-05.txt"))).isTrue();

    assertThat(Files.isRegularFile(root.resolve("home/file.ignore"))).isFalse();
    assertThat(Files.isRegularFile(root.resolve("home/large-file.bin"))).isTrue();
  }

  private void writeFile(Path path, Instant modifiedTime, String contents) throws IOException {
    Files.createDirectories(path.getParent());
    Files.newBufferedWriter(path).append(contents).close();
    Files.setLastModifiedTime(path, FileTime.from(modifiedTime));
  }
}

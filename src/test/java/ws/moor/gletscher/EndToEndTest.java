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

import com.google.common.base.Preconditions;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.CommandContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class EndToEndTest {

  @Test
  public void testSomething() throws Exception {
    FileSystem fs = Jimfs.newFileSystem();
    CloudFileStorage inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    Files.write(fs.getPath("/config.properties"),
        ("version: 1\n" +
            "cache_dir: /tmp/cache\n" +
            "max_split_size: 65536\n" +
            "disable_cache: true\n" +
            "split_algorithm: rolling\n" +
            "secret_key: !!binary AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n").getBytes(StandardCharsets.UTF_8));
    Files.createDirectories(fs.getPath("/tmp/cache"));

    // initial backup
    createRandomFileTree(fs.getPath("/home/pmoor"), 4);
    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "backup", "-c", "/config.properties", "/home/pmoor");

    // incremental backup
    createRandomFileTree(fs.getPath("/", "home", "pmoor", "new child"), 2);
    createRandomFileTree(fs.getPath("/", "home", "cmoor"), 3);
    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "backup", "-c", "/config.properties", "/home/pmoor", "/home/cmoor");

    // restore
    runCommandAndAssertSuccess(fs, inMemoryStorage,
        "restore", "-c", "/config.properties", "/tmp/restore");

    compare(fs.getPath("/", "home", "pmoor"), fs.getPath("/", "tmp", "restore", "home", "pmoor"));
    compare(fs.getPath("/", "home", "cmoor"), fs.getPath("/", "tmp", "restore", "home", "cmoor"));
  }

  private void runCommandAndAssertSuccess(FileSystem fs, CloudFileStorage inMemoryStorage, String... args) throws Exception {
    TestCommandContext context = new TestCommandContext(fs, inMemoryStorage);
    GletscherMain gletscherMain = new GletscherMain(context);
    gletscherMain.run(args);
    assertThat(context.status).isEqualTo(0);
  }

  private void compare(Path path1, Path path2) throws IOException {
    Preconditions.checkState(Files.exists(path1, LinkOption.NOFOLLOW_LINKS));
    boolean twoExists = Files.exists(path2, LinkOption.NOFOLLOW_LINKS);

    if (!twoExists) {
      System.out.println("missing: " + path1);
      return;
    }

    if (Files.isDirectory(path1, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.isDirectory(path2, LinkOption.NOFOLLOW_LINKS)) {
        System.out.println("not a directory: " + path2);
        return;
      }

      Iterator<Path> it = Files.list(path1).sorted().iterator();
      while (it.hasNext()) {
        Path path = path1.relativize(it.next());

        compare(path1.resolve(path), path2.resolve(path));
      }
    } else if (Files.isSymbolicLink(path1)) {
      if (!Files.isSymbolicLink(path2)) {
        System.out.println("not a symlink: " + path2);
        return;
      }

      Path target1 = Files.readSymbolicLink(path1);
      Path target2 = Files.readSymbolicLink(path2);
      if (!target1.equals(target2)) {
        System.out.printf("targets don't match for %s (%s): %s vs. %s\n", path1, path2, target1, target2);
        return;
      }
    } else if (Files.isRegularFile(path1, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.isRegularFile(path2, LinkOption.NOFOLLOW_LINKS)) {
        System.out.println("not a regular file: " + path2);
        return;
      }

      long size1 = Files.size(path1);
      long size2 = Files.size(path2);
      if (size1 != size2) {
        System.out.printf("file sizes don't match for %s (%s): %d vs. %d\n", path1, path2, size1, size2);
        return;
      }

      FileTime lastModifiedTime1 = Files.getLastModifiedTime(path1);
      FileTime lastModifiedTime2 = Files.getLastModifiedTime(path2);
      if (!lastModifiedTime1.equals(lastModifiedTime2)) {
        System.out.printf("last modified times do not match for %s (%s): %s vs. %s\n", path1, path2, lastModifiedTime1, lastModifiedTime2);
        return;
      }
    } else {
      System.out.println("unsupported type: " + path1);
    }
  }

  private void createRandomFileTree(Path root, int maxLevels) throws IOException {
    Preconditions.checkState(!Files.exists(root, LinkOption.NOFOLLOW_LINKS));
    Files.createDirectories(root);

    List<String> names = new ArrayList<>();

    Random rnd = new Random();
    int numFiles = 1 + rnd.nextInt(Math.max(2, 10 - maxLevels));
    for (int i = 0; i < numFiles; i++) {
      String name = createRandomFileName(rnd);
      names.add(name);
      Path path = root.resolve(name);
      int size = rnd.nextInt(256 << 10);
      byte[] data = new byte[size];
      rnd.nextBytes(data);

      Files.write(path, data, StandardOpenOption.CREATE_NEW);
    }

    int numDirs = rnd.nextInt(maxLevels);
    for (int i = 0; i < numDirs; i++) {
      String name = createRandomDirectoryName(rnd);
      names.add(name);
      createRandomFileTree(root.resolve(name), maxLevels - 1);
    }

    int numSymlinks = rnd.nextInt(names.size());
    for (int i = 0; i < numSymlinks; i++) {
      String name = createRandomSymlinkName(rnd);
      names.add(name);

      String target = rnd.nextBoolean() ? names.get(rnd.nextInt(names.size())) : "../something/random";

      Files.createSymbolicLink(root.resolve(name), root.relativize(root.resolve(target)));
    }
  }

  private String createRandomSymlinkName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextLong()));
  }

  private String createRandomDirectoryName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextDouble()));
  }

  private String createRandomFileName(Random rnd) {
    return String.valueOf(Math.abs(rnd.nextInt()));
  }

  private static class TestCommandContext implements CommandContext {
    private final FileSystem fs;
    private final CloudFileStorage cloudStorage;
    private Integer status = null;

    TestCommandContext(FileSystem fs, CloudFileStorage cloudStorage) {
      this.fs = fs;
      this.cloudStorage = cloudStorage;
    }

    @Override
    public FileSystem getFileSystem() {
      return fs;
    }

    @Override
    public InputStream getStdIn() {
      return System.in;
    }

    @Override
    public PrintStream getStdOut() {
      return System.out;
    }

    @Override
    public PrintStream getStdErr() {
      return System.err;
    }

    @Override
    public Clock getClock() {
      return Clock.systemDefaultZone();
    }

    @Override
    public ListeningExecutorService getExecutor() {
      return MoreExecutors.newDirectExecutorService();
    }

    @Override
    public void exit(int status) {
      Preconditions.checkState(this.status == null);
      this.status = status;
    }

    @Override
    public CloudFileStorage connectToCloud(Configuration config) {
      return cloudStorage;
    }
  }
}

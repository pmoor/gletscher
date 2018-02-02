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

package ws.moor.gletscher.files;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class FileSystemReaderTest {

  @Test
  public void testMultipleRootsOnWindows() throws Exception {
    Configuration config = Configuration.windows().toBuilder().setRoots("C:\\", "D:\\").build();
    FileSystem fs = Jimfs.newFileSystem(config);
    Set<Path> paths =
        ImmutableSet.of(
            fs.getPath("C:\\home\\pmoor"), fs.getPath("D:\\root"), fs.getPath("D:\\var\\log"));
    for (Path path : paths) {
      Files.createDirectories(path);
    }
    Files.createFile(fs.getPath("C:\\home\\pmoor\\should-be-included.TXT"));
    Files.createDirectories(fs.getPath("C:\\home\\pmoor\\child"));
    Files.createFile(fs.getPath("C:\\home\\pmoor\\child\\should-be-included.TXT"));
    Files.createFile(fs.getPath("C:\\home\\do-not-include.TXT"));
    Files.createFile(fs.getPath("D:\\do-not-include.TXT"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    FileSystemReader reader = new FileSystemReader(paths, new PrintStream(baos));

    StringBuilder output = new StringBuilder();
    AtomicInteger level = new AtomicInteger(0);
    Map<Path, Integer> sum =
        reader.start(
            new FileSystemReader.Visitor<Integer>() {
              @Override
              public Integer visit(
                  Path directory,
                  List<FileSystemReader.Entry> entries,
                  FileSystemReader.Recursor<Integer> recursor) {
                output.append(Strings.repeat(" ", level.get()));
                output.append(directory.toString()).append("\n");
                level.incrementAndGet();
                int sum = 1;
                for (FileSystemReader.Entry entry : entries) {
                  sum += recursor.recurse(entry.path);
                }
                level.decrementAndGet();
                output.append(Strings.repeat(" ", level.get()));
                output.append(sum).append("\n");
                return sum;
              }
            });

    assertThat(output.toString())
        .isEqualTo(
            "C:\\\n"
                + " C:\\home\n"
                + "  C:\\home\\pmoor\n"
                + "   C:\\home\\pmoor\\child\n"
                + "    C:\\home\\pmoor\\child\\should-be-included.TXT\n"
                + "    1\n"
                + "   2\n"
                + "   C:\\home\\pmoor\\should-be-included.TXT\n"
                + "   1\n"
                + "  4\n"
                + " 5\n"
                + "6\n"
                + "D:\\\n"
                + " D:\\root\n"
                + " 1\n"
                + " D:\\var\n"
                + "  D:\\var\\log\n"
                + "  1\n"
                + " 2\n"
                + "4\n");
    assertThat(sum).hasSize(2);
    assertThat(sum).containsEntry(fs.getPath("C:\\"), 6);
    assertThat(sum).containsEntry(fs.getPath("D:\\"), 4);
  }
}

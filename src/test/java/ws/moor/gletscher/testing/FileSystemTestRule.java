/*
 * Copyright 2021 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.testing;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

public class FileSystemTestRule extends ExternalResource {
  private final FileSystem fs;

  public static FileSystemTestRule unix() {
    return new FileSystemTestRule(Configuration.unix());
  }

  public static FileSystemTestRule windows() {
    return new FileSystemTestRule(Configuration.windows().toBuilder().setRoots("C:\\", "A:\\", "B:\\").build());
  }

  private FileSystemTestRule(Configuration configuration) {
    this.fs = Jimfs.newFileSystem(configuration);
  }

  public void createDirectories(String path) throws Exception {
    Files.createDirectories(getPath(path));
  }

  public void writeFile(String path, String contents) throws Exception {
    Files.writeString(getPath(path), contents);
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public Path getPath(String path, String... addtl) {
    return fs.getPath(path, addtl);
  }

  public void recursiveCopy(String srcPathString, FileSystem destFs, String destPathString) throws Exception {
    Path srcPath = getPath(srcPathString);
    Path destPath = destFs.getPath(destPathString);

    Files.walkFileTree(srcPath, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        Files.createDirectories(toDestPath(dir));
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.copy(file, toDestPath(file), StandardCopyOption.COPY_ATTRIBUTES);
        return FileVisitResult.CONTINUE;
      }

      private Path toDestPath(Path dir) {
        Path relativeSrcPath = srcPath.relativize(dir);
        Path dst = destPath;
        for (int i = 0; i < relativeSrcPath.getNameCount(); i++) {
          dst = dst.resolve(relativeSrcPath.getName(i).toString());
        }
        return dst;
      }
    });
  }
}

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

package ws.moor.gletscher.catalog;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.testing.FileSystemTestRule;

import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(JUnit4.class)
public class CatalogPathTest {
  @Rule
  public final FileSystemTestRule unix = FileSystemTestRule.unix();
  @Rule
  public final FileSystemTestRule windows = FileSystemTestRule.windows();

  @Test
  public void testUnixRoot() {
    CatalogPath root = CatalogPath.fromLocalPath(unix.getPath("/"));
    assertThat(root.isRoot()).isTrue();
    assertThat(root.asRootName()).isEqualTo("/");
    assertThrows(IllegalStateException.class, root::getParent);
    assertThrows(IllegalStateException.class, root::getFileName);
    assertPathEquals(root.toNativePath(unix.getFileSystem()), unix.getPath("/"));
    assertThat(root).isEqualTo(CatalogPath.fromRootName("/"));

    assertThrows(InvalidPathException.class, () -> root.toNativePath(windows.getFileSystem()));

    assertThrows(IllegalArgumentException.class, () -> CatalogPath.fromLocalPath(unix.getPath("relative")));
  }

  @Test
  public void testWindowsRoot() {
    CatalogPath root = CatalogPath.fromLocalPath(windows.getPath("C:\\"));
    assertThat(root.isRoot()).isTrue();
    assertThat(root.asRootName()).isEqualTo("C:\\");
    assertThrows(IllegalStateException.class, root::getParent);
    assertThrows(IllegalStateException.class, root::getFileName);
    assertPathEquals(root.toNativePath(windows.getFileSystem()), windows.getPath("C:\\"));
    assertThat(root).isEqualTo(CatalogPath.fromRootName("C:\\"));

    assertThrows(IllegalStateException.class, () -> root.toNativePath(unix.getFileSystem()));

    assertThrows(IllegalArgumentException.class, () -> CatalogPath.fromLocalPath(windows.getPath("relative")));
  }

  @Test
  public void testPrefixMapping() {
    CatalogPath src = CatalogPath.fromLocalPath(windows.getPath("A:\\Home\\Windows\\File.txt"));

    // Unchanged if not a prefix.
    assertThat(src.replacePrefix(CatalogPath.fromLocalPath(windows.getPath("C:\\Not\\A\\Prefix")), CatalogPath.fromLocalPath(windows.getPath("B:\\")))).isSameInstanceAs(src);

    assertThat(src.replacePrefix(CatalogPath.fromLocalPath(windows.getPath("A:\\Home")), CatalogPath.fromLocalPath(unix.getPath("/home"))))
        .isEqualTo(CatalogPath.fromLocalPath(unix.getPath("/home/Windows/File.txt")));
  }

  @Test
  public void testUnixStringParsing() {
    basicAssertions(unix.getFileSystem(), "/");
    basicAssertions(unix.getFileSystem(), "/home");
    basicAssertions(unix.getFileSystem(), "/home/pmoor");
    basicAssertions(unix.getFileSystem(), "/home/pmoor/");
    basicAssertions(unix.getFileSystem(), "/home/pmoor/test.txt");

    assertThrows(IllegalArgumentException.class, () -> CatalogPath.fromHumanReadableString(""));
  }

  @Test
  public void testWindowsStringParsing() {
    basicAssertions(windows.getFileSystem(), "A:\\");
    basicAssertions(windows.getFileSystem(), "B:\\Documents\\1.txt");
    basicAssertions(windows.getFileSystem(), "B:\\Documents\\");
    basicAssertions(windows.getFileSystem(), "C:\\Home\\Patrick\\cv.txt");

    assertThrows(IllegalArgumentException.class, () -> CatalogPath.fromHumanReadableString("A:"));
  }

  private static void basicAssertions(FileSystem fs, String path) {
    CatalogPath fromString = CatalogPath.fromHumanReadableString(path);
    assertThat(fromString).isEqualTo(CatalogPath.fromLocalPath(fs.getPath(path)));
    assertThat(fromString.makeChild("child")).isEqualTo(CatalogPath.fromLocalPath(fs.getPath(path, "child")));
    if (!fromString.isRoot()) {
      assertThat(fromString.getParent()).isEqualTo(CatalogPath.fromLocalPath(fs.getPath(path).getParent()));
      assertThat(fromString.getFileName()).isEqualTo(fs.getPath(path).getFileName().toString());
    }
    assertThat(CatalogPath.fromHumanReadableString(fromString.getHumanReadableString())).isEqualTo(fromString);
    assertPathEquals(fs.getPath(fromString.getHumanReadableString()), fs.getPath(path));
  }

  private static void assertPathEquals(Path actual, Path expected) {
    assertThat((Object) actual).isEqualTo(expected);
  }
}

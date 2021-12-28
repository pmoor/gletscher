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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Representation of an absolute path in a catalog.
 *
 * We cannot use {@link Path} directly since it can only represent paths on the platform we're running on. In the
 * catalog we may be dealing with paths from multiple platforms though.
 */
public class CatalogPath {
  private static final Pattern ROOT_PATTERN = Pattern.compile("/|[A-Z]:\\\\");

  private final Mode mode;
  private final String root;
  private final String[] components;

  private CatalogPath(String root, String[] components) {
    validateRoot(root);
    this.mode = "/".equals(root) ? Mode.UNIX : Mode.WINDOWS;
    this.root = root;

    for (String component : components) {
      validateComponent(mode, component);
    }
    this.components = components;
  }

  private static void validateRoot(String root) {
    checkArgument(ROOT_PATTERN.matcher(root).matches());
  }

  private static void validateComponent(Mode mode, String component) {
    checkArgument(!component.isEmpty());
    switch (mode) {
      case UNIX -> checkArgument(!component.contains("/"));
      case WINDOWS -> checkArgument(!component.contains("\\"));
      default -> throw new AssertionError(mode);
    }
  }

  public static CatalogPath fromLocalPath(Path path) {
    checkArgument(path.isAbsolute(), "path is not absolute: %s", path);
    String root = path.getRoot().toString();
    String[] parts = new String[path.getNameCount()];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = path.getName(i).toString();
    }
    return new CatalogPath(root, parts);
  }

  public static CatalogPath fromRootName(String rootName) {
    return new CatalogPath(rootName, new String[0]);
  }

  /**
   * This should only be used for parsing human entered input. For dealing with paths obtained through other means
   * {@link #fromLocalPath(Path)} is preferred.
   */
  public static CatalogPath fromHumanReadableString(String str) {
    // TODO: This entire method is a bit ugly...
    checkArgument(!str.isEmpty());
    if (str.charAt(0) == '/') {
      return new CatalogPath("/", Splitter.on('/').omitEmptyStrings().splitToList(str.substring(1)).toArray(new String[0]));
    } else {
      int idx = str.indexOf(":\\");
      return new CatalogPath(str.substring(0, idx + 2), Splitter.on('\\').omitEmptyStrings().splitToList(str.substring(idx + 2)).toArray(new String[0]));
    }
  }

  public CatalogPath makeChild(String fileName) {
    checkArgument(!fileName.isEmpty());
    String[] newComponents = Arrays.copyOf(components, components.length + 1);
    newComponents[newComponents.length - 1] = fileName;
    return new CatalogPath(root, newComponents);
  }

  public String asRootName() {
    checkState(isRoot());
    return root;
  }

  public boolean isRoot() {
    return components.length == 0;
  }

  public CatalogPath getParent() {
    checkState(!isRoot(), "the root does not have a parent");
    return new CatalogPath(root, Arrays.copyOf(components, components.length - 1));
  }

  public String getFileName() {
    checkState(!isRoot(), "the root does not have a file name");
    return components[components.length - 1];
  }

  public Path toNativePath(FileSystem fs) {
    Path result = fs.getPath(root, components);
    checkState(result.isAbsolute(), "resulting path was not absolute: %s", result);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CatalogPath that = (CatalogPath) o;
    return root.equals(that.root) && Arrays.equals(components, that.components);
  }

  @Override
  public int hashCode() {
    return 31 * root.hashCode() + Arrays.hashCode(components);
  }

  @Override
  public String toString() {
    return String.format("CatalogPath<root=%s,components=%s>", root, Arrays.toString(components));
  }

  public int approximateByteUsage() {
    return (3 + components.length) * 16 + root.length() + Arrays.stream(components).mapToInt(String::length).sum();
  }

  /**
   * Should only be used for display purposes. If the path needs to be used programmatically
   * {@link #toNativePath(FileSystem)} is preferred.
   */
  public String getHumanReadableString() {
    if (root.equals("/")) {
      return root + Joiner.on('/').join(components);
    } else {
      return root + Joiner.on('\\').join(components);
    }
  }

  public boolean isPrefixOf(CatalogPath path) {
    if (!root.equals(path.root) || components.length > path.components.length) {
      return false;
    }

    for (int i = 0; i < components.length; i++) {
      if (!components[i].equals(path.components[i])) {
        return false;
      }
    }

    return true;
  }

  public CatalogPath replacePrefix(CatalogPath prefix, CatalogPath replacementPrefix) {
    if (!prefix.isPrefixOf(this)) {
      return this;
    }

    CatalogPath result = replacementPrefix;
    for (int i = prefix.components.length; i < this.components.length; i++) {
      result = result.makeChild(this.components[i]);
    }
    return result;
  }

  private enum Mode {
    UNIX, WINDOWS;
  }
}

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

package ws.moor.gletscher.files;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

public class FileSystemReader {

  public interface Visitor<T> {
    T visit(Path directory, List<Entry> entries, Recursor<T> recursor);
  }

  public interface Recursor<T> {
    T recurse(Path directory);
  }

  private final Set<Path> paths;
  private final PrintStream stderr;

  public FileSystemReader(Set<Path> paths, PrintStream stderr) throws IOException {
    this.paths = new TreeSet<>();
    for (Path path : paths) {
      this.paths.add(path.toRealPath());
    }
    this.stderr = stderr;
  }

  public <T> Map<Path, T> start(Visitor<T> visitor) {
    Multimap<Path, Path> pathToRoots = TreeMultimap.create();
    Set<Path> roots = new TreeSet<>();
    for (Path root : paths) {
      Path current = root;
      while (current.getParent() != null) {
        pathToRoots.put(current.getParent(), current);
        current = current.getParent();
      }
      Preconditions.checkState(root.getRoot().equals(current));
      roots.add(current);
    }
    removeRecursively(pathToRoots, paths);

    Recursor<T> recursor = new Recursor<T>() {
      @Override public T recurse(Path directory) {
        List<Entry> entries = new ArrayList<>();

        try {
          if (pathToRoots.containsKey(directory)) {
            // artificial directory, use only contents from in here
            for (Path child : pathToRoots.get(directory)) {
              BasicFileAttributes attributes = readAttributes(child);
              entries.add(new Entry(child, attributes));
            }
          } else {
            if (Files.isReadable(directory)) {
              try (Stream<Path> stream = Files.list(directory)) {
                stream.forEach(child -> {
                  BasicFileAttributes attributes = readAttributes(child);
                  entries.add(new Entry(child, attributes));
                });
              }
            } else {
              stderr.println("unreadable: " + directory);
            }
          }
        } catch (IOException | RuntimeException e) {
          stderr.printf("error while reading directory %s: %s\n", directory, e);
        }
        Collections.sort(entries);
        return visitor.visit(directory, entries, this);
      }
    };

    Map<Path, T> result = new TreeMap<>();
    for (Path root : roots) {
      result.put(root, recursor.recurse(root));
    }
    return result;
  }

  private BasicFileAttributes readAttributes(Path child) {
    try {
      return Files.readAttributes(child, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeRecursively(Multimap<Path, Path> map, Set<Path> keysToRemove) {
    for (Path key : keysToRemove) {
      Set<Path> children = ImmutableSet.copyOf(map.get(key));
      if (!children.isEmpty()) {
        map.removeAll(key);
        removeRecursively(map, children);
      }
    }
  }

  public static final class Entry implements Comparable<Entry> {
    public final Path path;
    public final BasicFileAttributes attributes;

    Entry(Path path, BasicFileAttributes attributes) {
      this.path = path;
      this.attributes = attributes;
    }

    @Override
    public String toString() {
      return String.format("%s(%s)", path, attributes);
    }

    @Override
    public int compareTo(Entry o) {
      return this.path.compareTo(o.path);
    }

    public boolean isDirectory() {
      return attributes.isDirectory();
    }

    public boolean isRegularFile() {
      return attributes.isRegularFile();
    }

    public boolean isSymbolicLink() {
      return attributes.isSymbolicLink();
    }
  }
}

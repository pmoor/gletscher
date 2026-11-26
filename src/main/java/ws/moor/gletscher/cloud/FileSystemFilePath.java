/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.cloud;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import org.h2.message.DbException;
import org.h2.store.fs.FilePath;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * H2 {@link FilePath} provider backed by {@link java.nio.file.Path} so {@link
 * org.h2.mvstore.MVStore} persists and compacts on any NIO {@link FileSystem} (such as Jimfs).
 */
final class FileSystemFilePath extends FilePath {

  private static final String SCHEME = "gletscher-fs";
  private static final ConcurrentMap<String, FileSystem> BY_ID =
      new MapMaker().weakValues().makeMap();
  private static final ConcurrentMap<FileSystem, String> IDS =
      new MapMaker().weakKeys().makeMap();
  private static final AtomicLong NEXT_ID = new AtomicLong();
  private static volatile boolean registered = false;

  private final FileSystem fs;
  private final Path path;

  private FileSystemFilePath(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  /**
   * Returns a file name suitable for {@link org.h2.mvstore.MVStore} targeting {@code path}.
   * Default-filesystem paths are returned directly; non-default {@link FileSystem} paths are
   * routed through this {@link FilePath} provider.
   */
  static String toStoreName(Path path) {
    FileSystem fs = path.getFileSystem();
    if (fs.equals(FileSystems.getDefault())) {
      return path.toString();
    }
    ensureRegistered();
    String id =
        IDS.computeIfAbsent(
            fs,
            ignored -> {
              String newId = Long.toString(NEXT_ID.incrementAndGet());
              BY_ID.put(newId, fs);
              return newId;
            });
    // Ensure a retained strong reference on the reverse map stays consistent under weak values.
    BY_ID.putIfAbsent(id, fs);
    return SCHEME + ":" + id + ":" + path.toAbsolutePath();
  }

  private static void ensureRegistered() {
    if (!registered) {
      synchronized (FileSystemFilePath.class) {
        if (!registered) {
          FilePath.register(new FileSystemFilePath(null, null));
          registered = true;
        }
      }
    }
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FilePath getPath(String fullName) {
    int firstColon = fullName.indexOf(':');
    Preconditions.checkArgument(firstColon > 0, "Missing scheme: %s", fullName);
    int secondColon = fullName.indexOf(':', firstColon + 1);
    Preconditions.checkArgument(secondColon > firstColon, "Missing filesystem id: %s", fullName);

    String id = fullName.substring(firstColon + 1, secondColon);
    FileSystem targetFs =
        Preconditions.checkNotNull(BY_ID.get(id), "Unknown FileSystem id: %s", id);
    String rawPath = fullName.substring(secondColon + 1);
    if ("\\".equals(targetFs.getSeparator())) {
      // FilePath.get() replaces '\' with '/' before dispatching; restore Windows separators.
      rawPath = rawPath.replace('/', '\\');
    }
    FileSystemFilePath result = new FileSystemFilePath(targetFs, targetFs.getPath(rawPath));
    result.name = fullName;
    return result;
  }

  @Override
  public long size() {
    try {
      return Files.size(path);
    } catch (IOException e) {
      return 0L;
    }
  }

  @Override
  public void moveTo(FilePath newName, boolean atomicReplace) {
    Preconditions.checkArgument(
        newName instanceof FileSystemFilePath, "Cannot move across providers: %s", newName);
    Path target = ((FileSystemFilePath) newName).path;
    try {
      if (atomicReplace) {
        try {
          Files.move(
              path,
              target,
              StandardCopyOption.REPLACE_EXISTING,
              StandardCopyOption.ATOMIC_MOVE);
          return;
        } catch (AtomicMoveNotSupportedException ignored) {
          // Fall through to non-atomic replace below.
        }
        Files.move(path, target, StandardCopyOption.REPLACE_EXISTING);
      } else {
        Files.move(path, target);
      }
    } catch (IOException e) {
      throw DbException.convertIOException(e, name);
    }
  }

  @Override
  public boolean createFile() {
    try {
      Files.createFile(path);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    } catch (IOException e) {
      throw DbException.convertIOException(e, name);
    }
  }

  @Override
  public boolean exists() {
    return Files.exists(path);
  }

  @Override
  public void delete() {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw DbException.convertIOException(e, name);
    }
  }

  @Override
  public List<FilePath> newDirectoryStream() {
    try (Stream<Path> entries = Files.list(path)) {
      return entries.map(p -> getPath(toStoreName(p))).toList();
    } catch (IOException e) {
      throw DbException.convertIOException(e, name);
    }
  }

  @Override
  public FilePath toRealPath() {
    try {
      return getPath(toStoreName(path.toRealPath()));
    } catch (IOException e) {
      return getPath(toStoreName(path.toAbsolutePath().normalize()));
    }
  }

  @Override
  public FilePath getParent() {
    Path parent = path.getParent();
    return parent == null ? null : getPath(toStoreName(parent));
  }

  @Override
  public boolean isDirectory() {
    return Files.isDirectory(path);
  }

  @Override
  public boolean isRegularFile() {
    return Files.isRegularFile(path);
  }

  @Override
  public boolean isAbsolute() {
    return path.isAbsolute();
  }

  @Override
  public long lastModified() {
    try {
      return Files.getLastModifiedTime(path).toMillis();
    } catch (IOException e) {
      return 0L;
    }
  }

  @Override
  public boolean canWrite() {
    return Files.isWritable(path);
  }

  @Override
  public void createDirectory() {
    try {
      Files.createDirectory(path);
    } catch (IOException e) {
      throw DbException.convertIOException(e, name);
    }
  }

  @Override
  public FileChannel open(String mode) throws IOException {
    OpenOption[] options =
        switch (mode) {
          case "r" -> new OpenOption[] {StandardOpenOption.READ};
          case "rw" ->
              new OpenOption[] {
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE
              };
          case "rws" ->
              new OpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.SYNC
              };
          case "rwd" ->
              new OpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.DSYNC
              };
          default -> throw new IllegalArgumentException("Unsupported open mode: " + mode);
        };
    return FileChannel.open(path, options);
  }

  @Override
  public boolean setReadOnly() {
    return false;
  }
}

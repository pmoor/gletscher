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

package ws.moor.gletscher.cloud.testing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.testing.Testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class InMemoryCloudFileStorage implements CloudFileStorage {

  private final ListeningExecutorService executor;
  private final Object lock = new Object();
  private final SortedMap<String, Entry> files = new TreeMap<>();

  public InMemoryCloudFileStorage(ListeningExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix) {
    List<FileHeader> results = new ArrayList<>();
    synchronized (lock) {
      for (Entry entry : files.tailMap(prefix).values()) {
        if (entry.name.startsWith(prefix)) {
          results.add(entry.header);
        } else {
          break;
        }
      }
    }
    return results.iterator();
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    return executor.submit(() -> files.containsKey(name));
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    return executor.submit(() -> {
      synchronized (lock) {
        Entry entry = files.get(name);
        return entry == null ? null : entry.data.toByteArray();
      }
    });
  }

  @Override
  public void close() {
    // no-op
  }

  @Override public ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata) {
    return executor.submit(() -> {
      synchronized (lock) {
        if (files.containsKey(name)) {
          throw new FileAlreadyExistsException(name);
        }
        FileHeader header = new FileHeader(name, md5, data.length, ImmutableMap.copyOf(metadata));
        files.put(name, new Entry(name, header, ByteString.copyFrom(data)));
        return null;
      }
    });
  }

  public void delete(String name) {
    synchronized (lock) {
      Preconditions.checkArgument(files.containsKey(name), "file does not exist: %s", name);
      files.remove(name);
    }
  }

  public int getFileCount() {
    synchronized (lock) {
      return files.size();
    }
  }

  public long getTotalSize() {
    synchronized (lock) {
      return files.values().stream().collect(Collectors.summingInt(e -> e.data.size()));
    }
  }

  public void mergeFromProto(Testing.FileList fileList) {
    synchronized (lock) {
      for (Testing.File file : fileList.getFileList()) {
        Entry entry = Entry.fromProto(file);
        files.put(entry.name, entry);
      }
    }
  }

  public Testing.FileList toProto() {
    Testing.FileList.Builder fileListBuilder = Testing.FileList.newBuilder();
    synchronized (lock) {
      for (Entry entry : files.values()) {
        fileListBuilder.addFile(entry.toProto());
      }
    }
    return fileListBuilder.build();
  }

  private static class Entry {
    final String name;
    final FileHeader header;
    final ByteString data;

    private Entry(String name, FileHeader header, ByteString data) {
      this.name = name;
      this.header = header;
      this.data = data;
    }

    Testing.File toProto() {
      Testing.File.Builder builder = Testing.File.newBuilder();
      builder.setName(name);
      builder.setContents(data);
      builder.putAllMeta(header.metadata);
      return builder.build();
    }

    static Entry fromProto(Testing.File proto) {
      FileHeader header = new FileHeader(
          proto.getName(),
          Hashing.md5().hashBytes(proto.getContents().toByteArray()),
          proto.getContents().size(),
          proto.getMeta());
      return new Entry(proto.getName(), header, proto.getContents());
    }
  }
}

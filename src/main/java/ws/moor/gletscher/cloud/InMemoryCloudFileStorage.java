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

package ws.moor.gletscher.cloud;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class InMemoryCloudFileStorage implements CloudFileStorage {

  private final ListeningExecutorService executor;
  private final SortedMap<String, FileHeader> files = new TreeMap<>();
  private final Map<String, byte[]> data = new HashMap<>();

  public InMemoryCloudFileStorage(ListeningExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix) {
    List<FileHeader> results = new ArrayList<>();
    for (FileHeader header : files.tailMap(prefix).values()) {
      if (header.name.startsWith(prefix)) {
        results.add(header);
      } else {
        break;
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
      FileHeader header = files.get(name);
      if (header == null) {
        return null;
      }
      return data.get(name);
    });
  }

  @Override public ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata) {
    return executor.submit(() -> {
      if (files.containsKey(name)) {
        throw new FileAlreadyExistsException(name);
      }
      this.data.put(name, data.clone());
      files.put(name, new FileHeader(name, md5, data.length, ImmutableMap.copyOf(metadata)));
      return null;
    });
  }

  public int getFileCount() {
    return files.size();
  }

  public long getTotalSize() {
    long sum = 0;
    for (byte[] bytes : data.values()) {
      sum += bytes.length;
    }
    return sum;
  }
}

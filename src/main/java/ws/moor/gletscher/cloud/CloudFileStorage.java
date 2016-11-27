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

import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.Map;

public interface CloudFileStorage extends AutoCloseable {

  ListenableFuture<?> store(String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options);

  Iterator<FileHeader> listFiles(String prefix, int limit);

  ListenableFuture<Boolean> exists(String name);

  ListenableFuture<byte[]> get(String name);

  void close();

  class FileAlreadyExistsException extends Exception {
    public FileAlreadyExistsException(String name) {
      super("file already exists: " + name);
    }
  }

  class FileHeader {
    public final String name;
    public final HashCode md5;
    public final int size;
    public final Map<String, String> metadata;

    public FileHeader(String name, HashCode md5, int size, Map<String, String> metadata) {
      this.name = name;
      this.md5 = md5;
      this.size = size;
      this.metadata = metadata;
    }
  }

  class StoreOptions {
    public static StoreOptions DEFAULT = new StoreOptions(false);

    public final boolean cacheContentsOnUpload;

    public StoreOptions(boolean cacheContentsOnUpload) {
      this.cacheContentsOnUpload = cacheContentsOnUpload;
    }
  }
}

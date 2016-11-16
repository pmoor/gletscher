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

package ws.moor.gletscher.kv;

import javax.annotation.Nullable;
import java.util.Iterator;

public interface KVStore extends AutoCloseable {

  void store(Key key, byte[] value) throws KVStoreException;

  @Nullable byte[] get(Key key) throws KVStoreException;

  boolean contains(Key key) throws KVStoreException;

  void delete(Key key) throws KVStoreException;

  void flush() throws KVStoreException;

  @Override
  void close() throws KVStoreException;

  Iterator<Entry> iterator(Key start, boolean inclusive, boolean ascending);

  interface Entry {
    Key getKey();
    int size();
    @Nullable byte[] read() throws KVStoreException;
  }
}

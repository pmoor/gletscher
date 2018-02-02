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

package ws.moor.gletscher.kv;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

class MemoryLayer extends Layer {

  private final NavigableMap<Key, KeyEntry> keys = new TreeMap<>();
  private long approximateByteSize = 0;

  private static class KeyData implements Layer.KeyInfo {

    static final KeyData DELETED = new KeyData(null);

    static KeyData from(ByteBuffer value) {
      return new KeyData(Preconditions.checkNotNull(value));
    }

    private final ByteBuffer data;

    KeyData(ByteBuffer data) {
      this.data = data;
    }

    @Override
    public boolean isDeleteMarker() {
      return data == null;
    }

    @Override
    public int size() {
      Preconditions.checkState(!isDeleteMarker());
      return data.remaining();
    }

    @Override
    public ByteBuffer read() {
      Preconditions.checkState(!isDeleteMarker());
      return data.asReadOnlyBuffer();
    }
  }

  MemoryLayer(int id) throws KVStoreException {
    super(id);
  }

  @Override Iterator<KeyEntry> keyIterator(Key start, boolean inclusive, boolean ascending) {
    NavigableMap<Key, KeyEntry> map = ascending
        ? keys.tailMap(start, inclusive)
        : keys.headMap(start, inclusive).descendingMap();

    return map.values().iterator();
  }

  void write(Key key, ByteBuffer value) throws KVStoreException {
    keys.put(key, new KeyEntry(key, KeyData.from(value)));
    approximateByteSize += key.serializedSize() + value.remaining();
  }

  void delete(Key key) {
    keys.put(key, new KeyEntry(key, KeyData.DELETED));
    approximateByteSize += key.serializedSize();
  }

  @Override
  @Nullable KeyInfo find(Key key) {
    KeyEntry keyEntry = keys.get(key);
    return keyEntry != null ? keyEntry.info : null;
  }

  @Override
  void close() {
    throw new UnsupportedOperationException();
  }

  long getApproximateByteSize() {
    return approximateByteSize;
  }
}

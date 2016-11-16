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

class KeyEntry implements KVStore.Entry {
  final Key key;
  final BlockLocation location;

  KeyEntry(Key key, BlockLocation location) {
    this.key = key;
    this.location = location;
  }

  @Override public Key getKey() {
    return key;
  }

  @Override public int size() {
    return location.size;
  }

  @Nullable
  @Override
  public byte[] read() throws KVStoreException {
    // TODO(pmoor): handle layer unavailable/compacted/etc or key deleted
    return location.layer.read(location).array();
  }
}

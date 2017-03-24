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

package ws.moor.gletscher.kv;

import javax.annotation.Nullable;

class KeyEntry implements KVStore.Entry {
  final Key key;
  final Layer.KeyInfo info;

  KeyEntry(Key key, Layer.KeyInfo info) {
    this.key = key;
    this.info = info;
  }

  @Override public Key getKey() {
    return key;
  }

  @Override public int size() {
    return info.size();
  }

  @Nullable
  @Override
  public byte[] read() throws KVStoreException {
    // TODO(pmoor): handle layer unavailable/compacted/etc or key deleted
    return info.read().array();
  }
}

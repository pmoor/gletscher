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

import java.nio.ByteBuffer;
import java.util.Iterator;

abstract class Layer {

  protected final int id;

  Layer(int id) {
    this.id = id;
  }

  abstract BlockLocation find(Key key) throws KVStoreException;

  abstract void close() throws KVStoreException;

  abstract ByteBuffer read(BlockLocation location) throws KVStoreException;

  abstract Iterator<KeyEntry> keyIterator(Key start, boolean inclusive, boolean ascending);
}

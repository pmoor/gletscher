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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// >= always to the right
final class BTreeNode {
  private static final int MAX_NODE_SIZE = 32 << 10;

  final List<Key> keys;
  final List<BlockLocation> locations;
  final boolean isLeaf;

  private int serializedSize;

  BTreeNode(List<Key> keys, List<BlockLocation> locations) {
    this.keys = keys;
    this.locations = locations;
    this.isLeaf =
        keys.isEmpty()
            || !keys.get(0)
                .equals(
                    Key.MIN); // inner node always has empty key first (so that nothing is to the
    // left)

    serializedSize = 4;
    for (Key key : keys) {
      serializedSize += key.serializedSize();
    }
    for (BlockLocation location : locations) {
      serializedSize += location.serializedSize();
    }
  }

  public int size() {
    return keys.size();
  }

  int cacheMemoryUsage() {
    return serializedSize + keys.size() * 32;
  }

  boolean fits(Key key) {
    if (size() >= 2 && serializedSize + key.serializedSize() + 13 > MAX_NODE_SIZE) {
      return false;
    }
    return true;
  }

  static BTreeNode parseFrom(DiskLayer layer, ByteBuffer data) {
    int size = data.getInt();

    List<Key> keys = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      keys.add(Key.parseFrom(data));
    }
    List<BlockLocation> locations = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      locations.add(BlockLocation.parseFrom(layer, data));
    }

    return new BTreeNode(keys, locations);
  }

  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize);
    buffer.putInt(size());
    for (Key key : keys) {
      key.writeTo(buffer);
    }
    for (BlockLocation location : locations) {
      location.writeTo(buffer);
    }
    buffer.rewind();
    return buffer;
  }

  int findKeyIndex(Key key) {
    int index = Collections.binarySearch(keys, key);
    if (isLeaf) {
      return index;
    } else if (index < 0) {
      // inner
      int firstGreaterThan = -index - 1;
      return firstGreaterThan - 1;
    } else {
      return index;
    }
  }

  BlockLocation findKey(Key key) {
    int index = findKeyIndex(key);
    return index < 0 ? null : locations.get(index);
  }

  static BTreeNode newEmptyInnerNode() {
    ArrayList<Key> keys = new ArrayList<>();
    keys.add(Key.MIN);
    return new BTreeNode(keys, new ArrayList<>());
  }

  static BTreeNode newEmptyLeafNode() {
    return new BTreeNode(new ArrayList<>(), new ArrayList<>());
  }

  void addLeafEntry(Key key, BlockLocation location) {
    Preconditions.checkState(isLeaf);
    keys.add(key);
    locations.add(location);
    serializedSize += key.serializedSize() + location.serializedSize();
  }

  void addInnerKey(Key key) {
    Preconditions.checkState(!isLeaf);
    keys.add(key);
    serializedSize += key.serializedSize();
  }

  void addInnerChild(BlockLocation location) {
    Preconditions.checkState(!isLeaf);
    locations.add(location);
    serializedSize += location.serializedSize();
  }
}

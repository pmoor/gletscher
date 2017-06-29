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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DiskLayerWriter {

  static final long CURRENT_VERSION = 1;

  private final FileChannel out;

  private Key lastKey = Key.MIN;
  private RangeMap<Key, BlockLocation> nodes = TreeRangeMap.create();
  private BTreeNode leafNode = BTreeNode.newEmptyLeafNode();

  DiskLayerWriter(FileChannel out) throws IOException {
    this.out = out;
    writeHeader();
  }

  private void writeHeader() throws IOException {
    ByteBuffer version = ByteBuffer.allocate(8);
    version.putLong(CURRENT_VERSION);
    version.rewind();
    appendRaw(version);
  }

  void write(Key key, @Nullable ByteBuffer data) throws IOException {
    Preconditions.checkArgument(lastKey.compareTo(key) < 0);

    if (!leafNode.fits(key)) {
      ByteBuffer buffer = leafNode.serialize();
      BlockLocation leafLocation = append(BlockLocation.Type.NODE, buffer);
      nodes.put(toRange(leafNode), leafLocation);
      leafNode = BTreeNode.newEmptyLeafNode();
    }

    BlockLocation location;
    if (data == null) {
      location = new BlockLocation(BlockLocation.Type.DELETION, null, -1, -1);
    } else {
      location = append(BlockLocation.Type.VALUE, data);
    }
    leafNode.addLeafEntry(key, location);
  }

  void finish() throws IOException {
    ByteBuffer buffer = leafNode.serialize();
    BlockLocation leafLocation = append(BlockLocation.Type.NODE, buffer);
    nodes.put(toRange(leafNode), leafLocation);
    // Done with all leafs

    while (nodes.asMapOfRanges().size() > 1) {
      nodes = rollUp(nodes);
    }

    BlockLocation lastNodeAddress = Iterables.getOnlyElement(nodes.asMapOfRanges().values());

    ByteBuffer locationBuffer = lastNodeAddress.serialize();
    appendRaw(locationBuffer);
    locationBuffer.rewind();

    buffer = ByteBuffer.allocate(4);
    buffer.putInt(locationBuffer.remaining());
    buffer.rewind();
    appendRaw(buffer);
  }

  private RangeMap<Key, BlockLocation> rollUp(RangeMap<Key, BlockLocation> nodes) throws IOException {
    RangeMap<Key, BlockLocation> nextLevelNodes = TreeRangeMap.create();
    Range<Key> first = null;
    Range<Key> last = null;
    BTreeNode innerNode = BTreeNode.newEmptyInnerNode();
    for (Range<Key> range : nodes.asMapOfRanges().keySet()) {
      if (first == null) {
        first = range;
      }

      if (last != null) {
        Key between = last.upperEndpoint().findBetween(range.lowerEndpoint());
        if (!innerNode.fits(between)) {
          ByteBuffer buffer = innerNode.serialize();
          BlockLocation innerLocation = append(BlockLocation.Type.NODE, buffer);
          nextLevelNodes.put(first.span(last), innerLocation);

          innerNode = BTreeNode.newEmptyInnerNode();
          first = range;
        } else {
          innerNode.addInnerKey(between); // strictly less than goes to left
        }
      }

      innerNode.addInnerChild(nodes.get(range.lowerEndpoint()));
      last = range;
    }

    ByteBuffer buffer = innerNode.serialize();
    BlockLocation innerLocation = append(BlockLocation.Type.NODE, buffer);
    nextLevelNodes.put(first.span(last), innerLocation);
    return nextLevelNodes;
  }

  private Range<Key> toRange(BTreeNode leafNode) {
    Key min = leafNode.keys.get(0);
    Key max = leafNode.keys.get(leafNode.keys.size() - 1);
    return Range.closed(min, max);
  }

  BlockLocation append(BlockLocation.Type type, ByteBuffer data) throws IOException {
    int size = data.remaining();
    long position = appendRaw(data);
    return new BlockLocation(type, null, position, size);
  }

  long appendRaw(ByteBuffer data) throws IOException {
    long position = out.position();
    while (data.hasRemaining()) {
      out.write(data);
    }
    return position;
  }
}

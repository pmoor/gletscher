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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

class ReadWriteLayer extends Layer{

  private final Path rootDir;
  private final FileChannel fileChannel;
  private final NavigableMap<Key, BlockLocation> keys = new TreeMap<>();

  private long pos = 0;

  ReadWriteLayer(Path rootDir, int id) throws KVStoreException {
    super(id);
    this.rootDir = rootDir;
    try {
      fileChannel = FileChannel.open(
          rootDir.resolve("latest-data"),
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  Path finish() throws KVStoreException {
    Preconditions.checkArgument(!keys.isEmpty());

    RangeMap<Key, BlockLocation> nodes = TreeRangeMap.create();

    BTreeNode leafNode = BTreeNode.newEmptyLeafNode();
    for (Map.Entry<Key, BlockLocation> entry : keys.entrySet()) {
      if (!leafNode.fits(entry.getKey())) {
        ByteBuffer buffer = leafNode.serialize();
        BlockLocation leafLocation = append(BlockLocation.Type.NODE, buffer);
        nodes.put(toRange(leafNode), leafLocation);

        leafNode = BTreeNode.newEmptyLeafNode();
      }

      leafNode.addLeafEntry(entry.getKey(), entry.getValue());
    }

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

    Path newPath = rootDir.resolve(String.format("data-%06d", id));
    try {
      fileChannel.close();
      Files.move(rootDir.resolve("latest-data"), newPath, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
    return newPath;
  }

  private RangeMap<Key, BlockLocation> rollUp(RangeMap<Key, BlockLocation> nodes) throws KVStoreException {
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

  BlockLocation append(BlockLocation.Type type, ByteBuffer data) throws KVStoreException {
    int size = data.remaining();
    long position = appendRaw(data);
    return new BlockLocation(type, this, position, size);
  }

  long appendRaw(ByteBuffer data) throws KVStoreException {
    int size = data.remaining();
    long position = pos;
    pos += size;
    KVStoreImpl.writeToChannel(fileChannel, position, data);
    return position;
  }

  @Override
  ByteBuffer read(KeyInfo keyInfo) throws KVStoreException {
    Preconditions.checkArgument(keyInfo instanceof BlockLocation);
    BlockLocation location = (BlockLocation) keyInfo;
    Preconditions.checkState(location.layer == this);
    return KVStoreImpl.readFromChannel(fileChannel, location.offset, location.size);
  }

  @Override Iterator<KeyEntry> keyIterator(Key start, boolean inclusive, boolean ascending) {
    NavigableMap<Key, BlockLocation> map = ascending
        ? keys.tailMap(start, inclusive)
        : keys.headMap(start, inclusive).descendingMap();

    return Iterators.transform(
        map.entrySet().iterator(),
        (e) -> new KeyEntry(e.getKey(), e.getValue()));
  }

  void write(Key key, ByteBuffer value) throws KVStoreException {
    BlockLocation location = append(BlockLocation.Type.VALUE, value);
    keys.put(key, location);
  }

  void delete(Key key) {
    BlockLocation location = new BlockLocation(BlockLocation.Type.DELETION, this, -1, -1);
    keys.put(key, location);
  }

  @Override
  BlockLocation find(Key key) {
    return keys.get(key);
  }

  @Override
  void close() {
    throw new UnsupportedOperationException();
  }
}

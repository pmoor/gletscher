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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Stack;

class DiskLayer extends Layer {

  private final Path path;
  private FileChannel channel;
  private BlockLocation rootNodeLocation;

  private final static Cache<BlockLocation, BTreeNode> nodeCache = CacheBuilder.newBuilder()
      .weigher((Weigher<BlockLocation, BTreeNode>) (location, node) -> 32 + node.cacheMemoryUsage())
      .maximumWeight(64 << 20)
      .build();

  DiskLayer(Path path) {
    super(Integer.valueOf(path.getFileName().toString().substring("data-".length()), 10));
    Preconditions.checkState(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS));
    Preconditions.checkState(Files.isReadable(path));
    this.path = path;
  }

  void open() throws KVStoreException {
    try {
      channel = FileChannel.open(path, StandardOpenOption.READ);

      ByteBuffer buffer = KVStoreImpl.readFromChannel(channel, channel.size() - 4, 4);
      int length = buffer.getInt();

      buffer = KVStoreImpl.readFromChannel(channel, channel.size() - 4 - length, length);
      rootNodeLocation = BlockLocation.parseFrom(this, buffer);
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  void close() throws KVStoreException {
    try {
      channel.close();
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  void delete() throws KVStoreException {
    Preconditions.checkState(!channel.isOpen());
    try {
      Files.delete(path);
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  ByteBuffer read(KeyInfo keyInfo) throws KVStoreException {
    Preconditions.checkArgument(keyInfo instanceof BlockLocation);
    BlockLocation location = (BlockLocation) keyInfo;
    Preconditions.checkState(location.layer == this);
    return KVStoreImpl.readFromChannel(channel, location.offset, location.size);
  }

  @Override
  Iterator<KeyEntry> keyIterator(Key start, boolean inclusive, boolean ascending) {
    return new AbstractIterator<KeyEntry>() {
      boolean started = false;
      final Stack<StackElement> nodeStack = new Stack<>();

      @Override
      protected KeyEntry computeNext() {
        if (!started) {
          started = true;
          StackElement element = new StackElement(readNode(rootNodeLocation));
          nodeStack.push(element);

          while (true) {
            int index = element.node.findKeyIndex(start);
            if (index < 0) {
              Preconditions.checkState(element.node.isLeaf);
              // down in a leaf, but precise key not found, start at next higher
              element.nextPos = -index - (ascending ? 1 : 2);
              break;
            } else if (element.node.isLeaf) {
              element.nextPos = inclusive ? index : (ascending ? index + 1 : index - 1);
              break;
            } else {
              element.nextPos = ascending ? index + 1 : index - 1; // we're going down @index right now
              // descend
              StackElement child = new StackElement(readNode(element.node.locations.get(index)));
              nodeStack.push(child);
              element = child;
            }
          }
        }

        if (nodeStack.isEmpty()) {
          return endOfData();
        }

        StackElement top = nodeStack.peek();
        if (ascending && top.nextPos >= top.node.size() || !ascending && top.nextPos < 0) {
          // done with top
          nodeStack.pop();
          return computeNext();
        }

        if (top.node.isLeaf) {
          KeyEntry entry = new KeyEntry(
              top.node.keys.get(top.nextPos), top.node.locations.get(top.nextPos));
          if (ascending) {
            top.nextPos++;
          } else {
            top.nextPos--;
          }
          return entry;
        } else {
          BTreeNode child = readNode(top.node.locations.get(top.nextPos));
          if (ascending) {
            top.nextPos++;
          } else {
            top.nextPos--;
          }
          StackElement element = new StackElement(child);
          element.nextPos = ascending ? 0 : element.node.size() - 1;
          nodeStack.push(element);
          return computeNext();
        }
      }
    };
  }

  long size() throws KVStoreException {
    try {
      return channel.size();
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  private class StackElement {
    private final BTreeNode node;
    private int nextPos;

    private StackElement(BTreeNode node) {
      this.node = node;
      this.nextPos = 0;
    }
  }

  @Override
  BlockLocation find(Key key) throws KVStoreException {
    BTreeNode node = readNode(rootNodeLocation);
    while (true) {
      BlockLocation location = node.findKey(key);
      if (location == null || location.type != BlockLocation.Type.NODE) {
        return location;
      }
      node = readNode(location);
    }
  }

  private BTreeNode readNode(BlockLocation location) throws KVStoreException {
    Preconditions.checkArgument(location.type == BlockLocation.Type.NODE);
    BTreeNode node = nodeCache.getIfPresent(location);
    if (node == null) {
      ByteBuffer data = read(location);
      node = BTreeNode.parseFrom(location.layer, data);
      nodeCache.put(location, node);
    }
    return node;
  }
}

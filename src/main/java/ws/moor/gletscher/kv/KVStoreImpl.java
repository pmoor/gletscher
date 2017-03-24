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
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

class KVStoreImpl implements KVStore {

  private static final int MAX_LAYERS = 6;

  private final Path rootDir;
  private boolean opened = false;
  private boolean closed = false;

  private FileChannel lockFileChannel;
  private FileLock lockFileLock;

  private int nextId = 0;
  private final Deque<Layer> layers = new ArrayDeque<>();

  KVStoreImpl(Path rootDir) {
    this.rootDir = rootDir;
  }

  synchronized void open() throws KVStoreException {
    Preconditions.checkState(!opened);
    Preconditions.checkState(!closed);
    opened = true;

    try {
      lockFileChannel = FileChannel.open(rootDir.resolve("lock"), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE);
      lockFileLock = lockFileChannel.lock();

      for (Path path : Files.list(rootDir)
          .filter(p -> p.getFileName().toString().startsWith("data-"))
          .sorted()
          .collect(Collectors.toList())) {
        layers.push(openReadOnly(path));
      }
      nextId = layers.isEmpty() ? 0 : (layers.peek().id + 1);

      if (layers.size() >= MAX_LAYERS) {
        List<ReadOnlyLayer> layersToCompact = new ArrayList<>();
        ReadOnlyLayer first = (ReadOnlyLayer) layers.pop();
        layersToCompact.add(first);
        ReadOnlyLayer second = (ReadOnlyLayer) layers.pop();
        layersToCompact.add(second);
        long cumulativeSize = first.size() + second.size();

        while (!layers.isEmpty() && ((ReadOnlyLayer) layers.peek()).size() < cumulativeSize) {
          ReadOnlyLayer next = (ReadOnlyLayer) layers.pop();
          layersToCompact.add(next);
          cumulativeSize += next.size();
        }

        ReadWriteLayer newLayer = compact(layersToCompact, layers.isEmpty());
        layers.push(openReadOnly(newLayer.finish()));
        layersToCompact.forEach(Layer::close);
        layersToCompact.forEach(ReadOnlyLayer::delete);
      }
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  private ReadOnlyLayer openReadOnly(Path path) throws KVStoreException {
    ReadOnlyLayer readOnly = new ReadOnlyLayer(path);
    readOnly.open();
    return readOnly;
  }

  private static class Holder implements Comparable<Holder> {
    private final Layer file;
    private final Iterator<KeyEntry> it;
    private final boolean ascending;
    private KeyEntry current;

    private Holder(Layer file, Iterator<KeyEntry> it, boolean ascending) {
      this.file = file;
      this.it = it;
      this.ascending = ascending;
      if (it.hasNext()) {
        current = it.next();
      }
    }

    @Override public int compareTo(Holder other) {
      int cmp = current.key.compareTo(other.current.key);
      if (cmp == 0) {
        return -Integer.compare(file.id, other.file.id);
      }
      return ascending ? cmp : -cmp;
    }

    boolean moveOn() {
      if (it.hasNext()) {
        current = it.next();
        return true;
      }
      return false;
    }
  }

  private ReadWriteLayer compact(List<ReadOnlyLayer> layers, boolean major) throws KVStoreException {
    ReadWriteLayer combinedLayer = new ReadWriteLayer(rootDir, nextId++);

    PriorityQueue<Holder> queue = new PriorityQueue<>(layers.size());
    for (ReadOnlyLayer layer : layers) {
      Iterator<KeyEntry> it = layer.keyIterator(Key.MIN, true, true);
      if (it.hasNext()) {
        queue.add(new Holder(layer, it, true));
      }
    }

    Key lastKey = Key.MIN;
    while (!queue.isEmpty()) {
      Holder front = queue.poll();

      Key key = front.current.key;
      if (lastKey.compareTo(key) < 0) {
        Layer.KeyInfo keyInfo = front.current.info;
        if (!keyInfo.isDeleteMarker()) {
          // keep it
          combinedLayer.write(key, keyInfo.read());
        } else if (!major) {
          // keep deletions in minor compactions
          combinedLayer.delete(key);
        }
        lastKey = key;
      }

      if (front.moveOn()) {
        queue.add(front);
      }
    }

    return combinedLayer;
  }

  @Override
  public synchronized void close() throws KVStoreException {
    Preconditions.checkState(opened);
    Preconditions.checkState(!closed);

    flush();

    closed = true;
    while (!layers.isEmpty()) {
      layers.pop().close();
    }

    try {
      lockFileLock.release();
      lockFileChannel.close();
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  @Override
  public Iterator<Entry> iterator(Key start, boolean inclusive, boolean ascending) {
    return new AbstractIterator<Entry>() {
      boolean started = false;
      Key lastKey = null;
      PriorityQueue<Holder> queue = new PriorityQueue<>();

      @Override protected Entry computeNext() {
        if (!started) {
          started = true;
          for (Layer layer : layers) {
            Iterator<KeyEntry> it = layer.keyIterator(start, inclusive, ascending);
            if (it.hasNext()) {
              queue.add(new Holder(layer, it, ascending));
            }
          }
        }

        while (!queue.isEmpty()) {
          Holder front = queue.poll();
          KeyEntry currentEntry = front.current;
          if (front.moveOn()) {
            queue.add(front);
          }
          if (lastKey == null || !lastKey.equals(currentEntry.key)) {
            lastKey = currentEntry.key;
            if (!currentEntry.info.isDeleteMarker()) {
              return currentEntry;
            }
          }
        }

        return endOfData();
      }
    };
  }

  @Override
  public synchronized void store(Key key, byte[] value) throws KVStoreException {
    internalStore(key, ByteBuffer.wrap(value));
  }

  @Override
  public synchronized void delete(Key key) throws KVStoreException {
    Preconditions.checkArgument(key.isNormal());
    if (layers.isEmpty() || !(layers.peek() instanceof ReadWriteLayer)) {
      layers.push(new ReadWriteLayer(rootDir, nextId++));
    }
    ((ReadWriteLayer) layers.peek()).delete(key);
  }

  @Override
  public synchronized byte[] get(Key key) throws KVStoreException {
    Layer.KeyInfo keyInfo = find(key);
    if (keyInfo == null || keyInfo.isDeleteMarker()) {
      return null;
    }
    return keyInfo.read().array();
  }

  @Override
  public synchronized boolean contains(Key key) throws KVStoreException {
    Layer.KeyInfo keyInfo = find(key);
    return keyInfo != null && !keyInfo.isDeleteMarker();
  }

  @Override
  public synchronized void flush() throws KVStoreException {
    if (!layers.isEmpty() && layers.peek() instanceof ReadWriteLayer) {
      layers.push(openReadOnly(((ReadWriteLayer) layers.pop()).finish()));
    }
  }

  private void internalStore(Key key, ByteBuffer value) throws KVStoreException {
    Preconditions.checkArgument(key.isNormal());
    if (layers.isEmpty() || !(layers.peek() instanceof ReadWriteLayer)) {
      layers.push(new ReadWriteLayer(rootDir, nextId++));
    }
    ((ReadWriteLayer) layers.peek()).write(key, value);
  }

  private Layer.KeyInfo find(Key key) throws KVStoreException {
    for (Layer layer : layers) {
      Layer.KeyInfo keyInfo = layer.find(key);
      if (keyInfo != null) {
        return keyInfo;
      }
    }
    return null;
  }

  static void writeToChannel(FileChannel channel, long offset, ByteBuffer data) throws KVStoreException {
    try {
      channel.position(offset);
      while (data.hasRemaining()) {
        channel.write(data);
      }
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }

  static ByteBuffer readFromChannel(FileChannel channel, long offset, int size) throws KVStoreException {
    try {
      channel.position(offset);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      while (buffer.hasRemaining()) {
        channel.read(buffer);
      }
      buffer.rewind();
      return buffer;
    } catch (IOException e) {
      throw new KVStoreException(e);
    }
  }
}

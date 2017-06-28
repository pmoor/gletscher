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

import java.nio.ByteBuffer;

final class BlockLocation implements Layer.KeyInfo {
  static BlockLocation parseFrom(DiskLayer layer, ByteBuffer data) {
    Type type = Type.values()[data.get()];
    long offset = data.getLong();
    int size = data.getInt();
    return new BlockLocation(type, layer, offset, size);
  }

  ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize());
    writeTo(buffer);
    buffer.rewind();
    return buffer;
  }

  int serializedSize() {
    return 13;
  }

  void writeTo(ByteBuffer buffer) {
    buffer.put(type.value);
    buffer.putLong(offset);
    buffer.putInt(size);
  }

  @Override
  public boolean isDeleteMarker() {
    return type == Type.DELETION;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public ByteBuffer read() {
    Preconditions.checkState(!isDeleteMarker());
    return layer.read(this);
  }

  enum Type {
    NODE(0),
    VALUE(1),
    DELETION(2);

    private final byte value;

    Type(int value) {
      this.value = (byte) value;
    }
  }

  final Type type;
  final DiskLayer layer;
  final long offset;
  final int size;

  BlockLocation(Type type, DiskLayer layer, long offset, int size) {
    this.type = type;
    this.layer = layer;
    this.offset = offset;
    this.size = size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof BlockLocation)) {
      return false;
    }
    BlockLocation other = (BlockLocation) o;
    return this.offset == other.offset
        && this.size == other.size
        && this.layer == other.layer
        && this.type == other.type;
  }

  @Override
  public int hashCode() {
    return (int) ((layer.hashCode() * 31 + offset) * 31 + size);
  }
}

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
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

public final class Key implements Comparable<Key> {

  enum KeyType {
    NORMAL, MIN, MAX
  }

  public static final Key MIN = new Key(KeyType.MIN, new byte[0]);
  public static final Key MAX = new Key(KeyType.MAX, new byte[0]);

  private static final Comparator<byte[]> cmp = UnsignedBytes.lexicographicalComparator();
  private static final BaseEncoding encoding = BaseEncoding.base16().lowerCase();

  private final KeyType type;
  private final byte[] key;
  private int hashCode = 0;

  private Key(KeyType type, byte[] key) {
    if (type == KeyType.NORMAL) {
      Preconditions.checkArgument(key.length > 0);
    } else {
      Preconditions.checkArgument(key.length == 0);
    }
    this.type = type;
    this.key = key;
  }

  public static Key copyOf(byte[] key) {
    return new Key(KeyType.NORMAL, key.clone());
  }

  public static Key fromHex(String hex) {
    return new Key(KeyType.NORMAL, encoding.decode(hex));
  }

  public static Key fromUtf8(String string) {
    return copyOf(string.getBytes(StandardCharsets.UTF_8));
  }

  public static Key parseFrom(ByteBuffer buffer) {
    int length = buffer.getInt();
    if (length == 0) {
      return Key.MIN;
    } else if (length == -1) {
      return Key.MAX;
    } else {
      byte[] key = new byte[length];
      buffer.get(key);
      return new Key(KeyType.NORMAL, key);
    }
  }

  @Override
  public int compareTo(Key o) {
    if (this == MIN) {
      return o == MIN ? 0 : -1;
    } else if (this == MAX) {
      return o == MAX ? 0 : 1;
    } else if (o == MIN) {
      return 1;
    } else if (o == MAX) {
      return -1;
    } else {
      return cmp.compare(this.key, o.key);
    }
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      int code = Arrays.hashCode(key) + type.hashCode();
      hashCode = code != 0 ? code : 1;
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Key)) {
      return false;
    } else {
      Key otherKey = (Key) o;
      return this.type == otherKey.type
          && Arrays.equals(this.key, otherKey.key);
    }
  }

  @Override
  public String toString() {
    if (this == MIN) {
      return "<MIN>";
    } else if (this == MAX) {
      return "<MAX>";
    } else {
      return encoding.encode(key);
    }
  }

  public boolean isNormal() {
    return type == KeyType.NORMAL;
  }

  /**
   * @return lo < x <= hi
   */
  public Key findBetween(Key next) {
    Preconditions.checkArgument(isNormal());
    Preconditions.checkArgument(next.isNormal());
    Preconditions.checkArgument(compareTo(next) < 0);
    int min = Math.min(key.length, next.key.length);
    for (int i = 0; i < min; i++) {
      if (key[i] != next.key[i]) {
        return next.prefix(i + 1);
      }
    }
    return next.prefix(min + 1);
  }

  public Key prefix(int size) {
    Preconditions.checkArgument(size > 0);
    Preconditions.checkState(isNormal());
    return new Key(KeyType.NORMAL, Arrays.copyOfRange(key, 0, size));
  }

  public int serializedSize() {
    return 4 + key.length;
  }

  public void writeTo(ByteBuffer buffer) {
    if (type == KeyType.NORMAL) {
      buffer.putInt(key.length);
      buffer.put(key);
    } else {
      buffer.putInt(type == KeyType.MIN ? 0 : -1);
    }
  }

  /** Returns lexicographically next key. */
  public Key next() {
    Preconditions.checkState(isNormal());
    byte[] newKey = Arrays.copyOf(key, key.length + 1);
    return new Key(KeyType.NORMAL, newKey);
  }
}

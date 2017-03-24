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

package ws.moor.gletscher.blocks;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;

import javax.crypto.Mac;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public final class Signature implements Comparable<Signature> {

  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  private static final BaseEncoding STRING_ENCODING = BaseEncoding.base16().lowerCase();

  private static final int LENGTH = 32;

  private final byte[] signature;

  private Signature(byte[] signature) {
    Preconditions.checkArgument(signature.length == LENGTH);
    this.signature = signature;
  }

  public static Signature finalizeMac(Mac mac) {
    return new Signature(mac.doFinal());
  }

  public static Signature fromByteString(ByteString bytes) {
    return new Signature(bytes.toByteArray());
  }

  @Override public int compareTo(Signature o) {
    return COMPARATOR.compare(this.signature, o.signature);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(signature);
  }

  @Override public boolean equals(Object o) {
    if (o == null || !o.getClass().equals(Signature.class)) {
      return false;
    }
    return Arrays.equals(signature, ((Signature) o).signature);
  }

  @Override public String toString() {
    return STRING_ENCODING.encode(signature);
  }

  public static Signature fromString(String str) {
    Signature signature = new Signature(STRING_ENCODING.decode(str));
    Preconditions.checkArgument(signature.toString().equals(str), str);
    return signature;
  }

  public byte[] asBytes() {
    return signature.clone();
  }

  public ByteString asByteString() {
    return ByteString.copyFrom(signature);
  }

  public byte getFirstByte() {
    return signature[0];
  }

  public byte getSecondByte() {
    return signature[1];
  }
}

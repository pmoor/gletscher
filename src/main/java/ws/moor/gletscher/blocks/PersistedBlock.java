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

package ws.moor.gletscher.blocks;

import ws.moor.gletscher.proto.Gletscher;

public class PersistedBlock implements Comparable<PersistedBlock> {

  private final Signature signature;
  private final int originalLength;

  public PersistedBlock(Signature signature, int originalLength) {
    this.signature = signature;
    this.originalLength = originalLength;
  }

  @Override
  public String toString() {
    return String.format("%s:%d", signature, originalLength);
  }

  public int getOriginalLength() {
    return originalLength;
  }

  public Signature getSignature() {
    return signature;
  }

  @Override
  public int hashCode() {
    return signature.hashCode() * 37 + originalLength;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o.getClass().equals(PersistedBlock.class))) {
      return false;
    }
    PersistedBlock other = (PersistedBlock) o;
    return signature.equals(other.signature) && originalLength == other.originalLength;
  }

  public static PersistedBlock fromProto(Gletscher.PersistedBlock proto) {
    return new PersistedBlock(
        Signature.fromByteString(proto.getSignature()), proto.getOriginalSize());
  }

  public Gletscher.PersistedBlock toProto() {
    return Gletscher.PersistedBlock.newBuilder()
        .setSignature(signature.asByteString())
        .setOriginalSize(originalLength)
        .build();
  }

  @Override
  public int compareTo(PersistedBlock other) {
    int cmp = signature.compareTo(other.signature);
    return (cmp == 0) ? Integer.compare(originalLength, other.originalLength) : cmp;
  }
}

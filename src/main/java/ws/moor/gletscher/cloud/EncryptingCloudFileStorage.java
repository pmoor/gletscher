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

package ws.moor.gletscher.cloud;

import ws.moor.gletscher.util.Cryptor;

public class EncryptingCloudFileStorage extends TransformingCloudFileStorage {

  private final Cryptor cryptor;

  public EncryptingCloudFileStorage(CloudFileStorage delegate, Cryptor cryptor) {
    super(delegate);
    this.cryptor = cryptor;
  }

  @Override
  protected byte[] encode(byte[] data) {
    return cryptor.encrypt(data);
  }

  @Override
  protected byte[] decode(byte[] data) {
    return cryptor.decrypt(data);
  }
}

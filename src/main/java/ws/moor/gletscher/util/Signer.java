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

package ws.moor.gletscher.util;

import ws.moor.gletscher.blocks.Signature;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

public class Signer {

  public static final String MAC_ALGO = "HmacSha256";

  private final SecretKeySpec signingKey;

  public Signer(SecretKeySpec signingKey) {
    this.signingKey = signingKey;
  }

  public Signature computeSignature(byte[] data) {
    Mac mac = buildMac();
    mac.update(data);
    return Signature.finalizeMac(mac);
  }

  private Mac buildMac() {
    try {
      Mac mac = Mac.getInstance(MAC_ALGO);
      mac.init(signingKey);
      return mac;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}

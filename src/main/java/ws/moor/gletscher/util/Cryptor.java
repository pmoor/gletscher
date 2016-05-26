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

import com.google.common.base.Preconditions;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

public class Cryptor {

  private static final byte[] AES_CBC_PKCS5PADDING = new byte[] {1};
  public static final String KEY_ALGO = "AES";
  private static final String CIPHER_ALGO = "AES/CBC/PKCS5Padding";

  private final SecretKeySpec encryptionKey;

  public Cryptor(SecretKeySpec encryptionKey) {
    this.encryptionKey = encryptionKey;
  }

  public byte[] encrypt(byte[] plaintext) {
    try {
      Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
      cipher.init(Cipher.ENCRYPT_MODE, encryptionKey);

      IvParameterSpec ivSpec =
          cipher.getParameters().getParameterSpec(IvParameterSpec.class);
      byte[] iv = ivSpec.getIV();
      Preconditions.checkState(iv.length == 16, "odd iv length: " + iv.length);

      byte[] ciphertext = cipher.doFinal(plaintext);
      return MoreArrays.concatenate(AES_CBC_PKCS5PADDING, iv, ciphertext);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] decrypt(byte[] ciphertext) {
    try {
      Preconditions.checkArgument(MoreArrays.startsWith(ciphertext, AES_CBC_PKCS5PADDING));
      IvParameterSpec ivSpec = new IvParameterSpec(ciphertext, 1, 16);

      Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
      cipher.init(Cipher.DECRYPT_MODE, encryptionKey, ivSpec);
      return cipher.doFinal(ciphertext, 17, ciphertext.length - 17);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }
}

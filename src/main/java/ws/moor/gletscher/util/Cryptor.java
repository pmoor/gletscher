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

package ws.moor.gletscher.util;

import com.google.common.base.Preconditions;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public class Cryptor {

  private static final byte[] AES_CBC_PKCS5PADDING = new byte[] {1};
  private static final byte[] AES_CBC_PKCS5PADDING_HMACSHA256 = new byte[] {2};
  public static final String KEY_ALGO = "AES";
  private static final String CIPHER_ALGO = "AES/CBC/PKCS5Padding";
  public static final String MAC_ALGO = "HmacSha256";

  private final SecretKeySpec encryptionKey;
  private final SecretKeySpec signingKey;

  public Cryptor(SecretKeySpec encryptionKey, SecretKeySpec signingKey) {
    this.encryptionKey = encryptionKey;
    this.signingKey = signingKey;
  }

  public byte[] encrypt(byte[] plaintext) {
    try {
      Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
      cipher.init(Cipher.ENCRYPT_MODE, encryptionKey);

      IvParameterSpec ivSpec =
          cipher.getParameters().getParameterSpec(IvParameterSpec.class);
      byte[] iv = ivSpec.getIV();
      Preconditions.checkState(iv.length == 16, "odd iv length: " + iv.length);

      Mac mac = Mac.getInstance(MAC_ALGO);
      mac.init(signingKey);
      byte[] signature = mac.doFinal(plaintext);
      Preconditions.checkState(signature.length == 32, "odd signature length: " + signature.length);

      byte[] ciphertext = cipher.doFinal(plaintext);
      return MoreArrays.concatenate(AES_CBC_PKCS5PADDING_HMACSHA256, iv, signature, ciphertext);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] decrypt(byte[] ciphertext) {
    try {
      if (MoreArrays.startsWith(ciphertext, AES_CBC_PKCS5PADDING_HMACSHA256)) {
        Preconditions.checkArgument(ciphertext.length > 48);
        IvParameterSpec ivSpec = new IvParameterSpec(ciphertext, 1, 16);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, ivSpec);
        byte[] plaintext = cipher.doFinal(ciphertext, 49, ciphertext.length - 49);

        Mac mac = Mac.getInstance(MAC_ALGO);
        mac.init(signingKey);
        byte[] expectedSignature = mac.doFinal(plaintext);

        byte[] actualSignature = Arrays.copyOfRange(ciphertext, 17, 49);
        if (!Arrays.equals(expectedSignature, actualSignature)) {
          throw new IllegalArgumentException("expected signature does not match actual one");
        }

        return plaintext;
      } else if (MoreArrays.startsWith(ciphertext, AES_CBC_PKCS5PADDING)) {
        Preconditions.checkArgument(ciphertext.length > 16);
        IvParameterSpec ivSpec = new IvParameterSpec(ciphertext, 1, 16);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, ivSpec);
        return cipher.doFinal(ciphertext, 17, ciphertext.length - 17);
      } else {
        throw new IllegalArgumentException("unknown cryptor version: " + ciphertext[0]);
      }
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }
}

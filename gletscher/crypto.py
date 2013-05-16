# Copyright 2013 Patrick Moor <patrick@moor.ws>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import bz2
import hashlib
import hmac

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Cipher.XOR import XORCipher


class Crypter(object):
    IV_SIZE = AES.block_size

    CURRENT_SERIALIZATION_VERSION = 2
    NO_COMPRESSION_PREFIX = b"\x00"
    BZIP2_COMPRESSION_PREFIX = b"\x01"

    def __init__(self, secret_key):
        self._secret_key = secret_key

    def new_cipher(self, iv=None):
        if not iv:
            iv = Random.new().read(AES.block_size)
        assert len(iv) == AES.block_size
        return iv, AES.new(self._secret_key, AES.MODE_CFB, iv, segment_size=8)

    def encrypt(self, chunk, iv=None):
        iv, cipher = self.new_cipher(iv)
        return iv, cipher.encrypt(chunk)

    def decrypt(self, iv, ciphertext):
        _, cipher = self.new_cipher(iv)
        plaintext = cipher.decrypt(ciphertext)
        return plaintext

    def newHMAC(self):
        return hmac.new(self._secret_key, digestmod=hashlib.sha256)

    def hash(self, value):
        hmac = self.newHMAC()
        hmac.update(value)
        return hmac.digest()

    def EncryptChunk(self, digest, chunk):
        key = XORCipher(digest).encrypt(self._secret_key)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(key, AES.MODE_CFB, iv, segment_size=8)

        compressed_chunk = self._compressOrLeaveAlone(chunk)
        return iv + cipher.encrypt(compressed_chunk)

    def DecryptChunk(self, storage_version, digest, data):
        if storage_version == 1:
            iv = data[4:20]
            cipher = AES.new(self._secret_key, AES.MODE_CFB, iv, segment_size=8)
            return bz2.decompress(cipher.decrypt(data[20:]))
        elif storage_version == 2:
            key = XORCipher(digest).encrypt(self._secret_key)
            iv = data[:AES.block_size]
            cipher = AES.new(key, AES.MODE_CFB, iv, segment_size=8)

            compressed_chunk = cipher.decrypt(data[AES.block_size:])
            return self._decompressOrLeaveAlone(compressed_chunk)
        else:
            raise Exception("unknown storage version: %d" % storage_version)

    def _decompressOrLeaveAlone(self, compressed_chunk):
        if compressed_chunk[0] == 0:
            return compressed_chunk[1:]
        else:
            return bz2.decompress(compressed_chunk[1:])

    def _compressOrLeaveAlone(self, chunk):
        if len(chunk) < 128 * 1024:
            # chunk is small enough - simply compress the whole thing
            compressed = bz2.compress(chunk)
            if len(compressed) < len(chunk):
                # any saving is fine - we've already paid the cost of compression
                return Crypter.BZIP2_COMPRESSION_PREFIX + compressed
            else:
                return Crypter.NO_COMPRESSION_PREFIX + chunk
        else:
            # chunk is large - compress 64KB from the middle
            middle = len(chunk) // 2
            test_data = chunk[middle - 32 * 1024:middle + 32 * 1024]
            compressed = bz2.compress(test_data)
            if len(compressed) < 0.90 * len(test_data):
                return Crypter.BZIP2_COMPRESSION_PREFIX + bz2.compress(chunk)
            else:
                return Crypter.NO_COMPRESSION_PREFIX + chunk


class TreeHasher(object):
    BLOCK_SIZE = 1024 * 1024

    def __init__(self):
        self._current_hash = hashlib.sha256()
        self._digests = []
        self._length = 0

    def update(self, data):
        while len(data) > 0:
            remaining = TreeHasher.BLOCK_SIZE - (
                self._length % TreeHasher.BLOCK_SIZE)
            if len(data) >= remaining:
                chunk = data[:remaining]
                self._current_hash.update(chunk)
                self._digests.append(self._current_hash.digest())
                self._current_hash = hashlib.sha256()
                self._length += remaining
                data = data[remaining:]
            else:
                self._current_hash.update(data)
                self._length += len(data)
                data = ""

    def consume(self, f):
        chunk = f.read(TreeHasher.BLOCK_SIZE)
        while chunk:
            self.update(chunk)
            chunk = f.read(TreeHasher.BLOCK_SIZE)

    def get_tree_hash(self, start=0, end=None):
        if end is None:
            end = self._length
        assert (0 <= start < end <= self._length) or (
            start == end == self._length), "start must be smaller than end"
        assert start % TreeHasher.BLOCK_SIZE == 0, "must start on a block "\
                                                   "boundary"
        assert end == self._length or end % TreeHasher.BLOCK_SIZE == 0,\
        "must extend to the end or end on a block boundary"

        if end - start <= TreeHasher.BLOCK_SIZE:
            # single block
            block = start // TreeHasher.BLOCK_SIZE
            if block == len(self._digests):
                return self._current_hash.digest()
            else:
                return self._digests[block]
        else:
            t = TreeHasher.BLOCK_SIZE
            while start + 2 * t < end:
                t *= 2
            mid_point = start + t
            combo = hashlib.sha256()
            combo.update(self.get_tree_hash(start, mid_point))
            combo.update(self.get_tree_hash(mid_point, end))
            return combo.digest()

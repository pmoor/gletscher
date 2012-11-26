# Copyright 2012 Patrick Moor <patrick@moor.ws>
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

from Crypto import Random
from Crypto.Cipher import AES
import bz2
import hashlib
import hmac

class Crypter(object):
    IV_SIZE = AES.block_size

    def __init__(self, secret_key):
        assert len(secret_key) == 32
        self._secret_key = secret_key

    def new_cipher(self, iv=None):
        if not iv:
            iv = Random.new().read(AES.block_size)
        assert len(iv) == AES.block_size
        return iv, AES.new(self._secret_key, AES.MODE_CFB, iv)

    def encrypt(self, chunk, iv=None):
        iv, cipher = self.new_cipher(iv)
        return (iv, cipher.encrypt(bz2.compress(chunk)))

    def decrypt(self, iv, ciphertext):
        _, cipher = self.new_cipher(iv)
        plaintext = bz2.decompress(cipher.decrypt(ciphertext))
        return plaintext

    def newHMAC(self):
        return hmac.new(self._secret_key, digestmod=hashlib.sha256)

    def hash(self, value):
        hmac = self.newHMAC()
        hmac.update(value)
        return hmac.digest()


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
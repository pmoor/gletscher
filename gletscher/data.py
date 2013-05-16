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

import struct
from Crypto.Cipher import AES

IV_LENGTH = AES.block_size
VERSION_STRING = b"gletscher-data-v000"

class ReadOnlyDataFile(object):
    def __init__(self, file_name, crypter):
        self._crypter = crypter
        self._file = open(file_name, "rb")
        assert self._file.read(len(VERSION_STRING)) == VERSION_STRING

    def read(self, offset, length, digest=None):
        self._file.seek(offset)
        record_length, = struct.unpack(">L", self._file.read(4))
        assert record_length == length - 4

        iv = self._file.read(IV_LENGTH)
        ciphertext = self._file.read(length - 4 - IV_LENGTH)
        plaintext = self._crypter.decrypt(iv, ciphertext)
        if digest:
            assert digest == self._crypter.hash(plaintext)
        return plaintext

    def close(self):
        self._file.close()

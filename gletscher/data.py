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

import struct
from Crypto.Cipher import AES
from gletscher import crypto

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

class DataFileWriter(object):
    """
    The file starts with the string "gletscher-data-v000" and then
    contains a collection of records:

      [4 byte big endian length][16 byte AES IV][ciphertext]

    The length is the byte length of the IV and the ciphertext combined.
    """
    def __init__(self, file, crypter):
        self._file = file
        self._crypter = crypter

        self._tree_hasher = crypto.TreeHasher()
        self._offset = 0
        self._write(VERSION_STRING)
        self._chunks_written = 0

    def _write(self, *parts):
        written = 0
        for part in parts:
            self._file.write(part)
            self._tree_hasher.update(part)
            self._offset += len(part)
            written += len(part)
        return written

    def append_chunk(self, chunk):
        iv, ciphertext = self._crypter.encrypt(chunk)
        assert len(iv) == IV_LENGTH

        record_length = struct.pack(">L", len(iv) + len(ciphertext))
        start_offset = self._offset
        length = self._write(record_length, iv, ciphertext)
        self._chunks_written += 1
        return start_offset, length

    def close(self):
        self._file.close()
        return self._tree_hasher

    def bytes_written(self):
        return self._offset

    def chunks_written(self):
        return self._chunks_written

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

import bz2
import os
import struct
from gletscher.crypto import Crypter

def kv_pack(file_path, kv_mapping, crypter):
    with open(file_path, "wb") as f:
        iv, cipher = crypter.new_cipher()
        compressor = bz2.BZ2Compressor()
        hmac = crypter.newHMAC()

        f.write(iv)

        for index, db in kv_mapping.items():
            for k, v in generate_kv_pairs(db):
                entry = struct.pack(">BLL", index, len(k), len(v)) + k + v
                f.write(cipher.encrypt(compressor.compress(entry)))
                hmac.update(entry)

        f.write(cipher.encrypt(compressor.flush()))
        f.write(hmac.digest())

def kv_unpack(file_path, dbm_mapping, crypter):
    total_size = os.stat(file_path).st_size
    with open(file_path, "rb") as f:
        iv, cipher = crypter.new_cipher(f.read(Crypter.IV_SIZE))
        decompressor = bz2.BZ2Decompressor()
        hmac = crypter.newHMAC()

        buffer = b""
        while f.tell() < total_size - 32:
          raw = f.read(min(128 * 1024, total_size - 32 - f.tell()))
          buffer += decompressor.decompress(cipher.decrypt(raw))
          while len(buffer) >= 9:
            index, key_len, val_len = struct.unpack(">BLL", buffer[:9])
            if len(buffer) >= 9 + key_len + val_len:
                entry = buffer[:9 + key_len + val_len]
                hmac.update(entry)
                buffer = buffer[9:]
                key = buffer[:key_len]
                buffer = buffer[key_len:]
                value = buffer[:val_len]
                buffer = buffer[val_len:]

                # write the mapping
                dbm_mapping[index][key] = value
            else:
                break

        expected_signature = hmac.digest()
        signature = f.read(32)
        if expected_signature != signature:
            raise Exception("signatures do not match")

def generate_kv_pairs(db):
    k = db.firstkey()
    while k is not None:
        yield k, db[k]
        k = db.nextkey(k)
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
import struct
from gletscher.crypto import Crypter

VERSION_STRING = b"gletscher-kv-pack-v000"
ENTRY_TYPE_NEW_FILE = 1
ENTRY_TYPE_KV_PAIR = 2
ENTRY_TYPE_SIGNATURE = 3

def kv_pack(writer, kv_mapping, crypter):
    iv, cipher = crypter.new_cipher()
    compressor = bz2.BZ2Compressor()

    writer.write(VERSION_STRING + iv)

    for name, gen in kv_mapping.items():
        hmac = crypter.newHMAC()

        bname = str.encode(name)
        entry = struct.pack(">LBH", 7 + len(bname), ENTRY_TYPE_NEW_FILE, len(bname)) + bname
        writer.write(cipher.encrypt(compressor.compress(entry)))
        hmac.update(entry)

        for k, v in gen():
            entry = struct.pack(">LBLL", 13 + len(k) + len(v), ENTRY_TYPE_KV_PAIR, len(k), len(v)) + k + v
            writer.write(cipher.encrypt(compressor.compress(entry)))
            hmac.update(entry)

        entry = struct.pack(">LB", 37, ENTRY_TYPE_SIGNATURE) + hmac.digest()
        writer.write(cipher.encrypt(compressor.compress(entry)))

    writer.write(cipher.encrypt(compressor.flush()))

def kv_unpack(reader, crypter, file_start, file_done):
    if reader.read(len(VERSION_STRING)) != VERSION_STRING:
        raise Exception()

    iv, cipher = crypter.new_cipher(reader.read(Crypter.IV_SIZE))
    decompressor = bz2.BZ2Decompressor()

    current_file = None
    current_hmac = None

    buffer = bytearray()
    offset = 0
    while True:
        raw = reader.read(32 * 1024)
        if len(raw) > 0:
            buffer.extend(decompressor.decompress(cipher.decrypt(raw)))
        else:
            break

        while len(buffer) >= offset + 4:
            record_length, = struct.unpack(">L", buffer[offset:offset + 4])
            if len(buffer) < record_length + offset:
                del buffer[:offset]
                offset = 0
                break

            next_offset = offset + record_length
            current_record = bytes(buffer[offset:offset + record_length])
            if current_record[4] == ENTRY_TYPE_NEW_FILE:
              if current_file is not None:
                  raise Exception()
              _, _, file_name_length = struct.unpack(">LBH", current_record[:7])
              current_hmac = crypter.newHMAC()
              current_hmac.update(current_record)

              current_file = file_start(bytes.decode(current_record[7:7 + file_name_length]))
            elif current_record[4] == ENTRY_TYPE_KV_PAIR:
              if current_file is None:
                  raise Exception()
              _, _, key_length, value_length = struct.unpack(">LBLL", current_record[:13])
              current_hmac.update(current_record)

              key = current_record[13:13 + key_length]
              value = current_record[13 + key_length:13 + key_length + value_length]
              current_file[key] = value
            elif current_record[4] == ENTRY_TYPE_SIGNATURE:
              if current_file is None:
                  raise Exception()
              expected_hmac = current_record[5:]
              if expected_hmac != current_hmac.digest():
                  raise Exception()

              file_done(current_file)
              current_hmac = None
              current_file = None
            else:
              raise Exception()

            offset = next_offset
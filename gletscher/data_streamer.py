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

import json

import logging
import struct
from gletscher import hex, crypto

logger = logging.getLogger(__name__)

VERSION_STRING = b"gletscher-data-v000"


class DataStreamer(object):
    """
    The file starts with the string "gletscher-data-v000" and then
    contains a collection of records:

      [4 byte big endian length][16 byte AES IV][ciphertext]

    The length is the byte length of the IV and the ciphertext combined.
    """
    def __init__(self, index, glacier_client, crypter, backup_id):
        self._index = index
        self._glacier_client = glacier_client
        self._crypter = crypter
        self._backup_id = backup_id

        self._upload_chunk_size = 2 * 1024 * 1024
        self._max_file_size = 8 * 1024 * 1024
        self._max_pending_digests = 256 * 1024

        self._connection = None
        self._pending_upload = None
        self._pending_data = None
        self._pending_digests = None
        self._pending_tree_hasher = None
        self._pending_data_offset = None


    def upload(self, digest, chunk):
        if not self._pending_upload:
            self._start_new_upload()

        if digest in self._pending_digests:
            return

        logger.debug("new chunk: %s", hex.b2h(digest))

        iv, ciphertext = self._crypter.encrypt(chunk)
        record_length = struct.pack(">L", len(iv) + len(ciphertext))
        start_offset = self._pending_data_offset + len(self._pending_data)
        length = self._write_to_pending_data(record_length, iv, ciphertext)
        self._pending_digests[digest] = (start_offset, length, len(chunk))

        self._flush_pending_data()
        if (self._pending_data_offset > self._max_file_size
                or len(self._pending_digests) > self._max_pending_digests):
            self._finish_pending_upload()

    def finish(self):
        if self._pending_upload:
            self._flush_pending_data()
            self._finish_pending_upload()

    def _write_to_pending_data(self, *data):
        for d in data:
          self._pending_data += d
          self._pending_tree_hasher.update(d)
        return sum(len(d) for d in data)

    def _start_new_upload(self):
        self._connection = self._glacier_client.NewConnection()

        description = json.dumps({"backup": str(self._backup_id), "type": "data"})

        self._pending_upload = self._glacier_client._initiateMultipartUpload(
            self._connection, self._upload_chunk_size, description)
        self._pending_data = b""
        self._pending_digests = {}
        self._pending_tree_hasher = crypto.TreeHasher()
        self._pending_data_offset = 0

        self._write_to_pending_data(VERSION_STRING)

    def _flush_pending_data(self):
        while len(self._pending_data) >= self._upload_chunk_size:
            data = self._pending_data[0:self._upload_chunk_size]
            tree_hash = self._pending_tree_hasher.get_tree_hash(
                self._pending_data_offset, self._pending_data_offset + self._upload_chunk_size)

            print("uploading a chunk")
            self._connection = self._glacier_client._uploadPart(
                self._connection, self._pending_upload, data, hex.b2h(tree_hash), self._pending_data_offset)

            self._pending_data_offset += self._upload_chunk_size
            self._pending_data = self._pending_data[self._upload_chunk_size:]

    def _finish_pending_upload(self):
        if len(self._pending_data) > 0:
            tree_hash = self._pending_tree_hasher.get_tree_hash(
                self._pending_data_offset, self._pending_data_offset + len(self._pending_data))

            self._connection = self._glacier_client._uploadPart(
                self._connection, self._pending_upload, self._pending_data, hex.b2h(tree_hash), self._pending_data_offset)

            self._pending_data_offset += len(self._pending_data)
            self._pending_data = b""

        tree_hash = self._pending_tree_hasher.get_tree_hash()
        self._glacier_client._completeUpload(
            self._connection, self._pending_upload, hex.b2h(tree_hash), self._pending_data_offset)

        for digest, entry in self._pending_digests.items():
            self._index.add(digest, tree_hash, *entry)

        self._pending_upload = None
        self._pending_data = None
        self._pending_digests = {}
        self._pending_tree_hasher = None
        self._pending_data_offset = None
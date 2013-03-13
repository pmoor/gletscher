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
from gletscher import hex

logger = logging.getLogger(__name__)

VERSION_STRING = b"gletscher-data-v000"


class DataStreamer(object):
    """
    The file starts with the string "gletscher-data-v000" and then
    contains a collection of records:

      [4 byte big endian length][16 byte AES IV][ciphertext]

    The length is the byte length of the IV and the ciphertext combined.
    """
    def __init__(self, index, streaming_uploader, crypter, backup_id):
        self._index = index
        self._streaming_uploader = streaming_uploader
        self._crypter = crypter
        self._backup_id = backup_id

        self._max_file_size = 2 * 1024 * 1024 * 1024
        self._max_pending_digests = 256 * 1024

        self._pending_upload = None
        self._pending_digests = None

    def upload(self, digest, chunk):
        if not self._pending_upload:
            self._start_new_upload()

        if digest in self._pending_digests:
            return

        logger.debug("new chunk: %s", hex.b2h(digest))

        iv, ciphertext = self._crypter.encrypt(chunk)

        start_offset = self._pending_upload.bytes_written()

        length_prefix = struct.pack(">L", len(iv) + len(ciphertext))
        total_length = len(length_prefix) + len(iv) + len(ciphertext)

        if (start_offset + total_length > self._max_file_size
                or len(self._pending_digests) >= self._max_pending_digests):
            self._finish_upload()
            self._start_new_upload()

        self._pending_upload.write(length_prefix)
        self._pending_upload.write(iv)
        self._pending_upload.write(ciphertext)
        self._pending_digests[digest] = (start_offset, total_length, len(chunk))

    def finish(self):
        if self._pending_upload:
            self._finish_upload()

    def _start_new_upload(self):
        logger.info("starting a new upload")
        description = json.dumps({"backup": str(self._backup_id), "type": "data"})
        self._pending_upload = self._streaming_uploader.new_upload(description)
        self._pending_upload.write(VERSION_STRING)
        self._pending_digests = {}

    def _finish_upload(self):
        logger.info("finishing current upload")
        archive_id, tree_hash = self._pending_upload.finish()
        for digest, entry in self._pending_digests.items():
            self._index.add(digest, tree_hash, *entry)
        self._pending_upload = None
        self._pending_digests = None
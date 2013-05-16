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

import json
import logging
from Crypto import Random
from Crypto.Random import random
from gletscher import hex

logger = logging.getLogger(__name__)

# storage version 1: [4 byte big endian length prefix][16 byte AES IV][ciphertext]
#                    length is length of IV and ciphertext combined
#                    stored in index: offset, overall length (4 + 16 + len(ciphertext))
#                    ciphertext is aes256(bz2.compress(chunk))
#
# storage version 2: [(16 byte AES IV) ^ (first 16 bytes of digest)][ciphertext]
#                    ciphertext is aes256(compression indicator byte | (compressed) plaintext)
#                    compression indicator: 0 - no compression
#                                           1 - bz2.compress

class ChunkStreamer(object):

    def __init__(self, index, streaming_uploader, crypter, backup_id):
        self._index = index
        self._streaming_uploader = streaming_uploader
        self._crypter = crypter
        self._backup_id = backup_id

        self._max_file_size = 4 * 1024 * 1024 * 1024
        self._max_pending_digests = 256 * 1024

        self._pending_upload = None
        self._pending_digests = None

    def upload(self, digest, chunk):
        if not self._pending_upload:
            self._start_new_upload()

        if digest in self._pending_digests:
            return

        logger.debug("new chunk: %s", hex.b2h(digest))

        encrypted = self._crypter.EncryptChunk(digest, chunk)
        start_offset = self._pending_upload.bytes_written()

        if (start_offset + len(encrypted) > self._max_file_size
                or len(self._pending_digests) >= self._max_pending_digests):
            self._finish_upload()
            self._start_new_upload()

        self._pending_upload.write(encrypted)
        self._pending_digests[digest] = (
            self._crypter.CURRENT_SERIALIZATION_VERSION,
            start_offset, len(encrypted), len(chunk))

    def finish(self):
        if self._pending_upload:
            self._finish_upload()

    def _start_new_upload(self):
        logger.info("starting a new upload")
        description = json.dumps(
            {"backup": str(self._backup_id), "type": "data"},
            sort_keys=True)
        self._pending_upload = self._streaming_uploader.new_upload(description)
        self._pending_digests = {}
        # add a random amount of data at the beginning of the file
        random_data = Random.new().read(random.randint(0, 127))
        self._pending_upload.write(random_data)

    def _finish_upload(self):
        logger.info("finishing current upload")
        archive_id, tree_hash = self._pending_upload.finish()
        for digest, entry in self._pending_digests.items():
            self._index.add(digest, tree_hash, *entry)
        self._pending_upload = None
        self._pending_digests = None
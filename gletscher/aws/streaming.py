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

from concurrent import futures
import logging
from gletscher import hex, crypto

logger = logging.getLogger(__name__)

DEFAULT_THREAD_COUNT = 2
DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024
MAX_PENDING_FUTURES = 2

class PendingUpload(object):

    def __init__(self, streaming_uploader, upload_id):
        self._streaming_uploader = streaming_uploader
        self._upload_id = upload_id

        self._tree_hasher = crypto.TreeHasher()
        self._pending_data = bytearray()
        self._pending_data_offset = 0
        self._futures = set()

    def write(self, data):
        if not self._upload_id:
            raise Exception("upload not in progress")

        self._pending_data.extend(data)
        self._tree_hasher.update(data)

        while len(self._pending_data) >= self._streaming_uploader._block_size:
            self._flush_pending_data()

    def bytes_written(self):
        return self._pending_data_offset + len(self._pending_data)

    def finish(self):
        if not self._upload_id:
            raise Exception("upload not in progress")

        while len(self._pending_data) > 0:
            self._flush_pending_data()

        self._consume_futures()

        connection = self._streaming_uploader._glacier_client.NewConnection()
        tree_hash = self._tree_hasher.get_tree_hash()
        archive_id = self._streaming_uploader._glacier_client._completeUpload(
            connection, self._upload_id, hex.b2h(tree_hash), self._pending_data_offset)

        self._upload_id = None
        return archive_id, tree_hash

    def _flush_pending_data(self):
        length = min(self._streaming_uploader._block_size, len(self._pending_data))

        data = bytes(self._pending_data[0:length])
        tree_hash = self._tree_hasher.get_tree_hash(
            self._pending_data_offset, self._pending_data_offset + length)

        while len(self._futures) >= MAX_PENDING_FUTURES:
            logger.info("too many outstanding futures, waiting for completion")
            self._consume_futures(futures.FIRST_COMPLETED)

        self._futures.add(self._streaming_uploader._upload(
            self._upload_id, data, hex.b2h(tree_hash), self._pending_data_offset))

        self._pending_data_offset += length
        del self._pending_data[:length]

    def _consume_futures(self, return_when=futures.ALL_COMPLETED):
        completed, not_done = futures.wait(self._futures, return_when=return_when)
        for future in completed:
            future.exception()
            self._futures.remove(future)


class StreamingUploader(object):

    def __init__(self, glacier_client, block_size=DEFAULT_BLOCK_SIZE):
        self._glacier_client = glacier_client
        self._block_size = block_size
        self._executor = futures.ThreadPoolExecutor(DEFAULT_THREAD_COUNT)

    def new_upload(self, description):
        upload_id = self._glacier_client._initiateMultipartUpload(
            self._glacier_client.NewConnection(), self._block_size, description)
        return PendingUpload(self, upload_id)

    def finish(self):
        self._executor.shutdown()

    def _upload(self, upload_id, data, tree_hash, offset):
        logger.info("submitting an upload request: %s (%d/%d)" % (upload_id, offset, len(data)))
        return self._executor.submit(self._inner_upload, upload_id, data, tree_hash, offset)

    def _inner_upload(self, upload_id, data, tree_hash, offset):
        logger.info("uploading a part: %s (%d/%d)" % (upload_id, offset, len(data)))
        self._glacier_client._uploadPart(
            self._glacier_client.NewConnection(), upload_id, data, tree_hash, offset)

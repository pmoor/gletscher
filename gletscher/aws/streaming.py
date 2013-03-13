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

import threading
import queue
import logging
from gletscher import hex, crypto

logger = logging.getLogger(__name__)

DEFAULT_THREAD_COUNT = 2
DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024

class PendingUpload(object):

    def __init__(self, streaming_uploader, upload_id):
        self._streaming_uploader = streaming_uploader
        self._upload_id = upload_id

        self._tree_hasher = crypto.TreeHasher()
        self._pending_data = b""
        self._pending_data_offset = 0
        self._queue = queue.Queue(maxsize=self._streaming_uploader._thread_count)

    def write(self, data):
        if not self._upload_id:
            raise Exception("upload not in progress")

        self._pending_data += data
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

        self._queue.join()
        connection = self._streaming_uploader._glacier_client.NewConnection()
        tree_hash = self._tree_hasher.get_tree_hash()
        archive_id = self._streaming_uploader._glacier_client._completeUpload(
            connection, self._upload_id, hex.b2h(tree_hash), self._pending_data_offset)

        self._upload_id = None
        return archive_id, tree_hash

    def _flush_pending_data(self):
        length = min(self._streaming_uploader._block_size, len(self._pending_data))

        data = self._pending_data[0:length]
        tree_hash = self._tree_hasher.get_tree_hash(
            self._pending_data_offset, self._pending_data_offset + length)

        print("submitting work")
        self._queue.put(
            (self._upload_id, data, hex.b2h(tree_hash), self._pending_data_offset))
        self._streaming_uploader._queue.put(self._queue)

        self._pending_data_offset += length
        self._pending_data = self._pending_data[length:]


class StreamingUploader(object):

    def __init__(self,
                 glacier_client,
                 thread_count=DEFAULT_THREAD_COUNT,
                 block_size=DEFAULT_BLOCK_SIZE):
        self._glacier_client = glacier_client
        self._thread_count = thread_count
        self._block_size = block_size
        self._queue = queue.Queue(maxsize=thread_count)

        self._threads = [
            threading.Thread(target=self._thread_main, name="uploader-%d" % i, daemon=True)
            for i in range(thread_count)]
        for t in self._threads:
            t.start()

    def new_upload(self, description):
        upload_id = self._glacier_client._initiateMultipartUpload(
            self._glacier_client.NewConnection(), self._block_size, description)
        return PendingUpload(self, upload_id)

    def finish(self):
        for t in self._threads:
            self._queue.put(None)
        for t in self._threads:
            t.join()

    def _thread_main(self):
        connection = self._glacier_client.NewConnection()
        while True:
            work_queue = self._queue.get()
            if not work_queue:
                print("worker thread %s exiting" % threading.current_thread().name)
                break

            print("worker thread %s uploading a part" % threading.current_thread().name)
            work = work_queue.get()
            self._glacier_client._uploadPart(connection, *work)

            work_queue.task_done()
            self._queue.task_done()
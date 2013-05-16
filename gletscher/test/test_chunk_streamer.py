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

import hashlib
import unittest
import uuid
from gletscher.chunk_streamer import ChunkStreamer
from gletscher.crypto import Crypter
from gletscher.index import Index

SMALLEST_CHUNK_OF_DATA = b"!"
SMALL_CHUNK_OF_DATA = b"a small chunk of data!"

class FakeChunkStreamer(object):

    class FakePendingUpload(object):

        def __init__(self, description):
            self.description = description
            self.data = b""
            self.finished = False

        def write(self, data):
            assert not self.finished
            self.data += data

        def bytes_written(self):
            return len(self.data)

        def finish(self):
            assert not self.finished
            self.finished = True
            sha = hashlib.sha256()
            sha.update(self.data)
            self.tree_hash = sha.digest()
            sha.update(b"0")
            self.archive_id = sha.digest()

            return self.archive_id, self.tree_hash

    def __init__(self):
        self.uploads = []

    def new_upload(self, description):
        upload = FakeChunkStreamer.FakePendingUpload(description)
        self.uploads.append(upload)
        return upload


class TestChunkStreamer(unittest.TestCase):
    def test_write_two_compressed_chunks(self):
        crypter = Crypter(b"0" * 32)
        backup_id = uuid.UUID(int=42)
        fake_db = {}
        index = Index(fake_db)
        streaming_uploader = FakeChunkStreamer()

        streamer = ChunkStreamer(index, streaming_uploader, crypter, backup_id)
        self.assertEqual(0, len(streaming_uploader.uploads))

        streamer.upload(b"1" * 32, b"0" * 32 * 1024)
        self.assertEqual(1, len(streaming_uploader.uploads))
        self.assertEqual(0, len(fake_db))

        streamer.upload(b"2" * 32, b"0" * 256 * 1024)
        self.assertEqual(0, len(fake_db))

        streamer.finish()
        self.assertEqual(2, len(fake_db))

        upload = streaming_uploader.uploads[0]
        self.assertEqual(True, upload.finished)
        self.assertEqual(
            "{\"backup\": \"00000000-0000-0000-0000-00000000002a\", \"type\": \"data\"}",
            upload.description)

        first_entry = index.get(b"1" * 32)
        self.assertEqual(upload.tree_hash, first_entry.file_tree_hash)
        self.assertEqual(2, first_entry.storage_version)
        self.assertEqual(32 * 1024, first_entry.original_length)
        self.assertLessEqual(first_entry.offset, 128)
        self.assertLess(first_entry.persisted_length, 80)

        data = upload.data[first_entry.offset:first_entry.offset + first_entry.persisted_length]
        self.assertEqual(b"0" * 32 * 1024, crypter.DecryptChunk(2, b"1" * 32, data))

        second_entry = index.get(b"2" * 32)
        self.assertEqual(upload.tree_hash, second_entry.file_tree_hash)
        self.assertEqual(2, second_entry.storage_version)
        self.assertEqual(256 * 1024, second_entry.original_length)
        self.assertEqual(first_entry.offset + first_entry.persisted_length, second_entry.offset)
        self.assertLess(second_entry.persisted_length, 80)

        data = upload.data[second_entry.offset:second_entry.offset + second_entry.persisted_length]
        self.assertEqual(b"0" * 256 * 1024, crypter.DecryptChunk(2, b"2" * 32, data))

    def test_write_uncompressed_chunk(self):
        crypter = Crypter(b"7" * 32)
        backup_id = uuid.UUID(int=42)
        fake_db = {}
        index = Index(fake_db)
        streaming_uploader = FakeChunkStreamer()

        streamer = ChunkStreamer(index, streaming_uploader, crypter, backup_id)
        self.assertEqual(0, len(streaming_uploader.uploads))

        streamer.upload(b"\x01" * 32, b"1234567890")
        self.assertEqual(1, len(streaming_uploader.uploads))
        self.assertEqual(0, len(fake_db))

        streamer.finish()
        self.assertEqual(1, len(fake_db))

        upload = streaming_uploader.uploads[0]
        self.assertEqual(True, upload.finished)
        self.assertEqual(
            "{\"backup\": \"00000000-0000-0000-0000-00000000002a\", \"type\": \"data\"}",
            upload.description)

        entry = index.get(b"\x01" * 32)
        self.assertEqual(upload.tree_hash, entry.file_tree_hash)
        self.assertEqual(2, entry.storage_version)
        self.assertEqual(10, entry.original_length)
        self.assertLessEqual(entry.offset, 128)
        self.assertEqual(16 + 1 + 10, entry.persisted_length)

        data = upload.data[entry.offset:entry.offset + entry.persisted_length]
        self.assertEqual(b"1234567890", crypter.DecryptChunk(2, b"\x01" * 32, data))


if __name__ == '__main__':
    unittest.main()

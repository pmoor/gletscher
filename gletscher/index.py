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
import logging

logger = logging.getLogger(__name__)

CURRENT_INDEX_VERSION = 1

class IndexEntry(object):
    def __init__(self, file_tree_hash, storage_version, offset, persisted_length, original_length):
        assert len(file_tree_hash) == 32
        assert offset >= 0
        assert persisted_length >= 0
        assert original_length >= 0
        self.file_tree_hash = file_tree_hash
        self.storage_version = storage_version
        self.offset = offset
        self.persisted_length = persisted_length
        self.original_length = original_length

    def serialize(self):
        return struct.pack(
            ">BB32sQLL",
            CURRENT_INDEX_VERSION, self.storage_version,
            self.file_tree_hash, self.offset, self.persisted_length, self.original_length)

    @staticmethod
    def unserialize(packed):
        version, storage_version, tree_hash, offset, persisted_length, original_length = struct.unpack(
            ">BB32sQLL", packed)
        assert version == CURRENT_INDEX_VERSION
        return IndexEntry(tree_hash, storage_version, offset, persisted_length, original_length)


class Index(object):

    def __init__(self, db):
        self._db = db

    def get(self, digest):
        entry = self._db.get(digest)
        if entry:
            return IndexEntry.unserialize(entry)

    def contains(self, digest):
        if self._db.get(digest):
            return True

    def entries(self):
        k = self._db.firstkey()
        while k is not None:
            yield k, IndexEntry.unserialize(self._db[k])
            k = self._db.nextkey(k)

    def add(self, digest, file_tree_hash, storage_version, offset, persisted_length, original_length):
        if self.contains(digest):
            raise Exception()
        else:
            self._db[digest] = IndexEntry(
                file_tree_hash, storage_version, offset, persisted_length, original_length).serialize()

    def close(self):
        self._db.close()

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
import logging

logger = logging.getLogger(__name__)

NULL_TREE_HASH = b"\0" * 32

class IndexEntry(object):
    def __init__(self, file_tree_hash, offset, persisted_length, original_length):
        assert len(file_tree_hash) == 32
        assert offset >= 0
        assert persisted_length >= 0
        assert original_length >= 0
        self.file_tree_hash = file_tree_hash
        self.offset = offset
        self.persisted_length = persisted_length
        self.original_length = original_length

    def serialize(self):
        return struct.pack(">B32s3L", 1, self.file_tree_hash, self.offset, self.persisted_length, self.original_length)

    @staticmethod
    def unserialize(packed):
        version, tree_hash, offset, persisted_length, original_length = struct.unpack(
            ">B32s3L", packed)
        assert version == 1
        return IndexEntry(tree_hash, offset, persisted_length, original_length)


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

    def merge_temporary_index(self, tmp_index, file_tree_hash):
        assert len(file_tree_hash) == 32
        for digest, index_entry in tmp_index.tmp_entries():
            index_entry.file_tree_hash = file_tree_hash

            our_entry = self.get(digest)
            if our_entry:
                if our_entry.original_length != index_entry.original_length:
                    raise Exception()
                # trust our current entry
            else:
                self._db[digest] = index_entry.serialize()

    def close(self):
        self._db.close()


class TemporaryIndex(object):

    def __init__(self, db):
        self._db = db

    def contains(self, digest):
        if self._db.get(digest):
            return True

    def add(self, digest, offset, persisted_length, original_length):
        self._db[digest] = IndexEntry(
            NULL_TREE_HASH, offset, persisted_length, original_length).serialize()

    def close(self):
        self._db.close()

    def tmp_entries(self):
        k = self._db.firstkey()
        while k is not None:
            entry = IndexEntry.unserialize(self._db[k])
            assert len(k) == 32
            assert entry.file_tree_hash == NULL_TREE_HASH

            yield k, entry
            k = self._db.nextkey(k)
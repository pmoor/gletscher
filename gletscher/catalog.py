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

import io
import os
import stat
import struct

CURRENT_CATALOG_VERSION = 1

class _FakeStat(object):
    def __init__(self, mode, size, mtime, uid, gid):
        self.st_mode = mode
        self.st_size = size
        self.st_mtime = mtime
        self.st_uid = uid
        self.st_gid = gid


class CatalogEntry(object):
    def __init__(self, file_stat):
        self._mode = int(file_stat.st_mode)
        self._size = int(file_stat.st_size)
        self._mtime = int(file_stat.st_mtime)
        self._uid = int(file_stat.st_uid)
        self._gid = int(file_stat.st_gid)

    def has_changed(self, file_stat):
        return (int(file_stat.st_mode) != self._mode
                or int(file_stat.st_size) != self._size
                or int(file_stat.st_mtime) != self._mtime
                or int(file_stat.st_uid) != self._uid
                or int(file_stat.st_gid) != self._gid)

    def serialize(self):
        data = struct.pack(
            ">BLQQLL",
            CURRENT_CATALOG_VERSION,
            self._mode, self._size, self._mtime, self._uid, self._gid)
        return data

    def is_regular_file(self):
        return stat.S_ISREG(self._mode)

    def is_directory(self):
        return stat.S_ISDIR(self._mode)

    def is_link(self):
        return stat.S_ISLNK(self._mode)

    def set_size(self, new_size):
        self._size = int(new_size)

    @staticmethod
    def unserialize(entry):
        f = io.BytesIO(entry)
        version, *stat_values = struct.unpack(">BLQQLL", f.read(29))
        file_stat = _FakeStat(*stat_values)
        if stat.S_ISLNK(file_stat.st_mode):
            target_length, = struct.unpack(">L", f.read(4))
            target = f.read(target_length)
            return LinkCatalogEntry(file_stat, target)
        elif stat.S_ISREG(file_stat.st_mode):
            digest_count, = struct.unpack(">L", f.read(4))
            digests = []
            for i in range(digest_count):
                digests.append(f.read(32))
            return FileCatalogEntry(file_stat, digests)
        else:
            return CatalogEntry(file_stat)


class FileCatalogEntry(CatalogEntry):
    def __init__(self, file_stat, digests):
        super(FileCatalogEntry, self).__init__(file_stat)
        self._digests = tuple(digests)

    def serialize(self):
        data = super(FileCatalogEntry, self).serialize()
        data += struct.pack(">L", len(self._digests))
        for digest in self._digests:
            data += digest
        return data

    def digests(self):
        return self._digests


class LinkCatalogEntry(CatalogEntry):
    def __init__(self, file_stat, target):
        super(LinkCatalogEntry, self).__init__(file_stat)
        assert type(target) == bytes
        self._target = target

    def serialize(self):
        data = super(LinkCatalogEntry, self).serialize()
        data += struct.pack(">L", len(self._target))
        data += self._target
        return data

    def target(self):
        return self._target


class Catalog(object):
    def __init__(self, db):
        self._db = db

    def add_file(self, full_path, file_stat, digests, total_length):
        entry = FileCatalogEntry(file_stat, digests)
        # adjust total file size to match chunk length
        entry.set_size(total_length)

        self._db[full_path] = entry.serialize()

    def add(self, full_path, file_stat):
        if stat.S_ISLNK(file_stat.st_mode):
            self._db[full_path] = LinkCatalogEntry(
                file_stat, os.readlink(full_path)).serialize()
        else:
            self._db[full_path] = CatalogEntry(file_stat).serialize()

    def find(self, full_path):
        entry = self._db.get(full_path)
        if entry:
            return CatalogEntry.unserialize(entry)

    def close(self):
        self._db.close()

    def transfer(self, full_path, entry):
        self._db[full_path] = entry.serialize()

    def entries(self):
        k = self._db.firstkey()
        while k is not None:
            yield k, CatalogEntry.unserialize(self._db[k])
            k = self._db.nextkey(k)

    def match(self, or_patterns):
        for full_path in self._db.keys():
            matches = False
            for pattern in or_patterns:
                if pattern.search(full_path):
                    matches = True
                    break
            if matches:
                yield full_path, CatalogEntry.unserialize(self._db[full_path])

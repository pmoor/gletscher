import StringIO
import anydbm
import os
import stat
import struct
from datetime import datetime

class _FakeStat(object):
  def __init__(self, mode, size, mtime, uid, gid):
    self.st_mode = mode
    self.st_size = size
    self.st_mtime = mtime
    self.st_uid = uid
    self.st_gid = gid

class CatalogEntry(object):

  def __init__(self, full_path, file_stat):
    self._full_path = full_path
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
    encoded = self._full_path.encode("utf8")
    data = struct.pack(">L", len(encoded))
    data += encoded
    data += struct.pack(">LQQLL", self._mode, self._size, self._mtime, self._uid, self._gid)
    return data

  @staticmethod
  def unserialize(entry):
    f = StringIO.StringIO(entry)
    path_length, = struct.unpack(">L", f.read(4))
    full_path = f.read(path_length).decode("utf8")
    file_stat = _FakeStat(*struct.unpack(">LQQLL", f.read(28)))

    if stat.S_ISLNK(file_stat.st_mode):
      target_length, = struct.unpack(">L", f.read(4))
      target = f.read(target_length).decode("utf8")
      return LinkCatalogEntry(full_path, file_stat, target)
    elif stat.S_ISREG(file_stat.st_mode):
      digest_count, = struct.unpack(">L", f.read(4))
      digests = []
      for i in range(digest_count):
        digests.append(f.read(32))
      return FileCatalogEntry(full_path, file_stat, digests)
    else:
      return CatalogEntry(full_path, file_stat)


class FileCatalogEntry(CatalogEntry):
  def __init__(self, full_path, file_stat, digests):
    super(FileCatalogEntry, self).__init__(full_path, file_stat)
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
  def __init__(self, full_path, file_stat, target):
    super(LinkCatalogEntry, self).__init__(full_path, file_stat)
    self._target = target

  def serialize(self):
    data = super(LinkCatalogEntry, self).serialize()
    encoded = self._target.encode("utf8")
    data += struct.pack(">L", len(encoded))
    data += encoded
    return data

  def target(self):
    return self._target


class Catalog(object):

  def __init__(self, dir, catalog_name, truncate=False):
    full_path = os.path.join(dir, "%s.catalog" % catalog_name)
    mode = "c"
    if truncate:
      mode = "n"
    self._db = anydbm.open(full_path, mode)

  def add_file(self, full_path, file_stat, digests):
    self._db[full_path.encode("utf8")] = FileCatalogEntry(full_path, file_stat, digests).serialize()

  def add(self, full_path, file_stat):
    if stat.S_ISLNK(file_stat.st_mode):
      self._db[full_path.encode("utf8")] = LinkCatalogEntry(full_path, file_stat, os.readlink(full_path)).serialize()
    else:
      self._db[full_path.encode("utf8")] = CatalogEntry(full_path, file_stat).serialize()

  def find(self, full_path):
    entry = self._db.get(full_path.encode("utf8"))
    if entry:
      return CatalogEntry.unserialize(entry)

  def close(self):
    self._db.close()

  def transfer(self, full_path, entry):
    self._db[full_path.encode("utf8")] = entry.serialize()

  def raw_entries(self):
    return self._db.iteritems()
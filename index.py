import os
import struct
import logging
import anydbm
import time

logger = logging.getLogger(__name__)

class IndexEntry(object):

  def __init__(self, file_id, offset, persisted_length, original_length):
    assert file_id >= 0
    assert offset >= 0
    assert persisted_length >= 0
    assert original_length >= 0
    self.file_id = file_id
    self.offset = offset
    self.persisted_length = persisted_length
    self.original_length = original_length

  def serialize(self):
    return struct.pack(">4L", self.file_id, self.offset, self.persisted_length, self.original_length)

  @staticmethod
  def unserialize(packed):
    return IndexEntry(*struct.unpack(">4L", packed))

class FileEntry(object):

  def __init__(self, archive_id, tree_hash):
    self._archive_id = archive_id
    self._tree_hash = tree_hash

  def serialize(self):
    encoded = self._archive_id.encode("utf8")
    return struct.pack(">L", len(encoded)) + encoded + self._tree_hash


class Index(object):

  NEXT_FILE_ID_KEY = "_next-file-id"
  FILE_ENTRY_KEY_FORMAT = "_file-%d"

  def __init__(self, index_dir):
    self._index_dir = index_dir
    self._main_index_file = os.path.join(index_dir, "index.anydbm")
    self._db = anydbm.open(self._main_index_file, "w")
    self._cleanDb()

  def _cleanDb(self):
    logger.debug("starting db cleanup")
    start_time = time.time()
    valid_files = set()
    invalid_files = set()
    to_be_removed = set()

    for key, value in self._db.iteritems():
      if not key.startswith("_"):
        entry = IndexEntry.unserialize(value)
        if entry.file_id in valid_files:
          continue
        elif entry.file_id in invalid_files:
          to_be_removed.add(key)
        else:
          if Index.FILE_ENTRY_KEY_FORMAT % entry.file_id in self._db:
            valid_files.add(entry.file_id)
          else:
            invalid_files.add(entry.file_id)
            to_be_removed.add(key)

    if valid_files:
      logger.debug("valid file indices: %s", sorted(valid_files))
    if invalid_files:
      logger.debug("INvalid file indices: %s", sorted(invalid_files))

    logger.debug("analysis completed in %0.2fs", time.time() - start_time)

    if to_be_removed:
      logger.debug("removing %d index entries", len(to_be_removed))
      start_time = time.time()
      for key in to_be_removed:
        del self._db[key]
      self._db.sync()
      logger.debug("removed index entries in %0.2fs", time.time() - start_time)

  def reserve_file_id(self):
    next_id = int(self._db[Index.NEXT_FILE_ID_KEY])
    assert not Index.FILE_ENTRY_KEY_FORMAT % next_id in self._db
    self._db[Index.NEXT_FILE_ID_KEY] = "%d" % (next_id + 1)
    self._db.sync()
    return next_id

  def find(self, digest):
    entry = self._db.get(digest)
    if entry:
      return IndexEntry.unserialize(entry)

  def add(self, digest, position):
    self._db[digest] = IndexEntry(*position).serialize()

  def add_data_file(self, file_id, archive_id, tree_hash):
    key_name = Index.FILE_ENTRY_KEY_FORMAT % file_id
    assert not key_name in self._db
    self._db[key_name] = FileEntry(archive_id, tree_hash).serialize()
    self._db.sync()

  @staticmethod
  def CreateEmpty(index_dir):
    db = anydbm.open(os.path.join(index_dir, "index.anydbm"), "n")
    db[Index.NEXT_FILE_ID_KEY] = "1"
    db.close()

  def close(self):
    self._db.close()

  def raw_entries(self):
    return self._db.iteritems()

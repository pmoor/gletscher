import os
import struct
import logging
import dbm
import time
import xdrlib

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

  def __init__(self, archive_id=None, tree_hash=None):
    self.archive_id = archive_id
    self.tree_hash = tree_hash

  def serialize(self):
    packer = xdrlib.Packer()
    if self.archive_id:
      packer.pack_bool(True)
      packer.pack_string(str.encode(self.archive_id))
    else:
      packer.pack_bool(False)

    if self.tree_hash:
      packer.pack_bool(True)
      packer.pack_bytes(self.tree_hash)
    else:
      packer.pack_bool(False)

    return packer.get_buffer()

  @staticmethod
  def unserialize(packed):
    unpacker = xdrlib.Unpacker(packed)
    archive_id = None
    if unpacker.unpack_bool():
      archive_id = bytes.decode(unpacker.unpack_string())
    tree_hash = None
    if unpacker.unpack_bool():
      tree_hash = unpacker.unpack_bytes()
    return FileEntry(archive_id, tree_hash)


class Index(object):

  NEXT_FILE_ID_KEY = b"_next-file-id"
  FILE_ENTRY_KEY_FORMAT = "_file-%d"

  def __init__(self, index_dir, consistency_check=True):
    self._index_dir = index_dir
    self._main_index_file = os.path.join(index_dir, "index.anydbm")
    self._db = dbm.open(self._main_index_file, "w")
    if consistency_check:
      self._assert_consistent()

  def _constructFileEntryKey(self, file_id):
    key_name = Index.FILE_ENTRY_KEY_FORMAT % file_id
    return str.encode(key_name)

  def FindAllFileEntries(self):
    entries = {}
    for key in self._db.keys():
      if not key.startswith(b"_"):
        entry = IndexEntry.unserialize(self._db[key])
        if entry.file_id in entries:
          continue
        else:
          key_name = self._constructFileEntryKey(entry.file_id)
          if not key_name in self._db:
            entries[entry.file_id] = None
          else:
            entries[entry.file_id] = FileEntry.unserialize(self._db[key_name])
    return entries

  def _assert_consistent(self):
    logger.info("starting index consistency checks")
    start_time = time.time()
    valid_files = set()
    invalid_files = set()

    for key in self._db.keys():
      if not key.startswith(b"_"):
        entry = IndexEntry.unserialize(self._db[key])
        if entry.file_id in valid_files:
          continue
        else:
          key_name = self._constructFileEntryKey(entry.file_id)
          if not key_name in self._db:
            invalid_files.add(entry.file_id)
          else:
            existing_entry = FileEntry.unserialize(self._db[key_name])
            if existing_entry.tree_hash is None or existing_entry.archive_id is None:
              invalid_files.add(entry.file_id)
            else:
              valid_files.add(entry.file_id)

    if valid_files:
      logger.info("valid file indices: %s", sorted(valid_files))
    if invalid_files:
      logger.warning("invalid file indices: %s", sorted(invalid_files))

    logger.debug("analysis completed in %0.2fs", time.time() - start_time)

    if invalid_files:
      raise Exception("the index contains partial data file information, cleanup first")

  def reserve_file_slot(self):
    next_id = int(self._db[Index.NEXT_FILE_ID_KEY])
    while self._constructFileEntryKey(next_id) in self._db:
      next_id += 1
    self._db[Index.NEXT_FILE_ID_KEY] = "%d" % (next_id + 1)
    self._db.sync()
    return next_id

  def find(self, digest):
    entry = self._db.get(digest)
    if entry:
      return IndexEntry.unserialize(entry)

  def add(self, digest, chunk_length, file_slot, offset, persisted_length):
    self._db[digest] = IndexEntry(file_slot, offset, persisted_length, chunk_length).serialize()

  def add_data_file_pre_upload(self, file_id, tree_hash):
    key_name = self._constructFileEntryKey(file_id)
    assert not key_name in self._db
    self._db[key_name] = FileEntry(None, tree_hash).serialize()
    self._db.sync()

  def finalize_data_file(self, file_id, tree_hash, archive_id):
    key_name = self._constructFileEntryKey(file_id)
    existing_entry = FileEntry.unserialize(self._db[key_name])
    assert existing_entry.tree_hash == tree_hash
    assert existing_entry.archive_id is None

    existing_entry.archive_id = archive_id
    self._db[key_name] = existing_entry.serialize()
    self._db.sync()

  def RemoveAllEntriesForFile(self, file_id):
    remove = set()
    for key in self._db.keys():
      if not key.startswith(b"_"):
        entry = IndexEntry.unserialize(self._db[key])
        if entry.file_id == file_id:
          remove.add(key)

    logger.info("removing %d entries for file %d" % (len(remove), file_id))
    key_name = self._constructFileEntryKey(file_id)
    del self._db[key_name]
    for key in remove:
      del self._db[key]
    self._db.sync()

  @staticmethod
  def CreateEmpty(index_dir):
    db = dbm.open(os.path.join(index_dir, "index.anydbm"), "n")
    db[Index.NEXT_FILE_ID_KEY] = "1"
    db.close()

  def close(self):
    self._db.close()

  def raw_entries(self):
    return self._db.items()

  def GetArchiveId(self, file_id):
    key_name = self._constructFileEntryKey(file_id)
    existing_entry = FileEntry.unserialize(self._db[key_name])
    assert existing_entry.archive_id is not None
    return existing_entry.archive_id

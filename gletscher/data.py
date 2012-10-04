import os
import struct
from Crypto.Cipher import AES
import tempfile
from gletscher import hex, crypto

IV_LENGTH = AES.block_size
VERSION_STRING = b"gletscher-data-v000"

class ReadOnlyDataFile(object):

  def __init__(self, file_name, crypter):
    self._crypter = crypter
    self._file = open(file_name, "rb")
    assert self._file.read(len(VERSION_STRING)) == VERSION_STRING

  def read(self, offset, length, digest=None):
    self._file.seek(offset)
    record_length, = struct.unpack(">L", self._file.read(4))
    assert record_length == length - 4

    iv = self._file.read(IV_LENGTH)
    ciphertext = self._file.read(length - 4 - IV_LENGTH)
    plaintext = self._crypter.decrypt(iv, ciphertext)
    if digest:
      assert digest == self._crypter.hash(plaintext)
    return plaintext

  def close(self):
    self._file.close()


class DataFile(object):

  def __init__(self, dir, max_size, crypter):
    self._dir = dir
    self._max_size = int(max_size)
    self._crypter = crypter

    self._tree_hasher = crypto.TreeHasher()

    fd, self._file_name = tempfile.mkstemp(dir=dir)
    self._file = os.fdopen(fd, "wb")
    self._offset = 0
    self._write(VERSION_STRING)

  def _write(self, *parts):
    for part in parts:
      self._file.write(part)
      self._tree_hasher.update(part)
      self._offset += len(part)

  def add(self, chunk):
    assert self.fits(chunk), "chunk does not fit anymore"
    iv, ciphertext = self._crypter.encrypt(chunk)

    record_length = struct.pack(">L", len(iv) + len(ciphertext))
    start_offset = self._offset
    self._write(record_length, iv, ciphertext)

    return start_offset, (self._offset - start_offset)

  def fits(self, chunk):
    # approximate (ignoring compression/encryption)
    return self._offset + max(40, 1.05 * len(chunk)) + IV_LENGTH + 4 < self._max_size

  def finalize(self):
    self._file.close()
    self._file = None
    tree_hash = self.tree_hash()
    new_file_name = os.path.join(self._dir, "%s.data" % hex.b2h(tree_hash))
    os.rename(self._file_name, new_file_name)
    self._file_name = new_file_name
    return tree_hash

  def delete(self):
    os.unlink(self._file_name)

  def tree_hasher(self):
    return self._tree_hasher

  def tree_hash(self):
    return self._tree_hasher.get_tree_hash()

  def file_name(self):
    return self._file_name




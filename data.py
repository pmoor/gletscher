import os
from Crypto.Cipher import AES
import struct
import crypto
import tempfile

class DataFile(object):

  IV_LENGTH = AES.block_size

  def __init__(self, dir, max_size, crypter):
    self._dir = dir
    self._max_size = int(max_size)
    self._crypter = crypter

    self._tree_hasher = crypto.TreeHasher()

    fd, self._file_name = tempfile.mkstemp(dir=dir)
    self._file = os.fdopen(fd, "w")
    self._offset = 0

  def add(self, chunk):
    iv, ciphertext = self._crypter.encrypt(chunk)

    self._file.write(iv)
    self._tree_hasher.update(iv)

    self._file.write(ciphertext)
    self._tree_hasher.update(ciphertext)

    length = len(ciphertext) + DataFile.IV_LENGTH
    position = (self._offset, length)
    self._offset += length
    return position

  def fits(self, chunk):
    # approximate (ignoring compression/encryption)
    return self._offset + len(chunk) < self._max_size

  def is_empty(self):
    return not self._offset

  def finalize(self):
    self._file.close()
    self._file = None
    tree_hash = self.tree_hash()
    new_file_name = os.path.join(self._dir, "%s.data" % tree_hash.encode("hex"))
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

  def id(self):
    return self._file_id




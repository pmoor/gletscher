import os
from Crypto.Cipher import AES
import struct
import crypto

class DataFile(object):

  IV_LENGTH = AES.block_size

  def __init__(self, file_id, dir, max_size, crypter):
    self._dir = dir
    self._max_size = int(max_size)
    self._crypter = crypter

    self._tree_hasher = crypto.TreeHasher()

    self._file_id = file_id

    self._file_name = os.path.join(dir, "%06d.data.tmp" % self._file_id)
    self._file = open(self._file_name, "w")
    self._file.write(struct.pack(">L", self._file_id))
    self._tree_hasher.update(struct.pack(">L", self._file_id))
    self._offset = 4

  def _calculateIv(self, offset):
    hmac = self._crypter.newHMAC()
    hmac.update(struct.pack(">LL", self._file_id, offset))
    return hmac.digest()[:DataFile.IV_LENGTH]

  def add(self, chunk):
    _, ciphertext = self._crypter.encrypt(chunk, iv=self._calculateIv(self._offset))
    self._file.write(ciphertext)
    self._tree_hasher.update(ciphertext)

    length = len(ciphertext)
    position = (self._file_id, self._offset, length, len(chunk))
    self._offset += length
    return position

  def fits(self, chunk):
    # approximate (ignoring compression/encryption)
    return self._offset + len(chunk) < self._max_size

  def is_empty(self):
    return self._offset == 4

  def finalize(self):
    self._file.close()

  def delete(self):
    os.unlink(self._file_name)

  def tree_hasher(self):
    return self._tree_hasher

  def file_name(self):
    return self._file_name

  def id(self):
    return self._file_id




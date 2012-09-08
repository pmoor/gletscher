from Crypto import Random
from Crypto.Cipher import AES
import bz2
import hashlib
import hmac
import math

class Crypter(object):

  def __init__(self, secret_key):
    assert len(secret_key) == 32
    self._secret_key = secret_key

  def encrypt(self, chunk, iv=None):
    if not iv:
      iv = Random.new().read(AES.block_size)
    assert len(iv) == AES.block_size
    cipher = AES.new(self._secret_key, AES.MODE_CFB, iv)
    return (iv, cipher.encrypt(bz2.compress(chunk)))

  def decrypt(self, iv, ciphertext):
    cipher = AES.new(self._secret_key, AES.MODE_CFB, iv)
    plaintext = bz2.decompress(cipher.decrypt(ciphertext))
    return plaintext

  def newHMAC(self):
    return hmac.new(self._secret_key, digestmod=hashlib.sha256)

  def hash(self, value):
    hmac = self.newHMAC()
    hmac.update(value)
    return hmac.digest()


class TreeHasher(object):

  BLOCK_SIZE = 1024 * 1024

  def __init__(self):
    self._current_hash = hashlib.sha256()
    self._digests = []
    self._length = 0

  def update(self, data):
    while len(data) > 0:
      remaining = TreeHasher.BLOCK_SIZE - (self._length % TreeHasher.BLOCK_SIZE)
      if len(data) >= remaining:
        chunk = data[:remaining]
        self._current_hash.update(chunk)
        self._digests.append(self._current_hash.digest())
        self._current_hash = hashlib.sha256()
        self._length += remaining
        data = data[remaining:]
      else:
        self._current_hash.update(data)
        self._length += len(data)
        data = ""

  def get_tree_hash(self, start, end):
    if start == end == 0:
      h = hashlib.sha256()
      return h.digest()
    assert 0 <= start < end <= self._length, "start must be smaller than end"
    assert start % TreeHasher.BLOCK_SIZE == 0, "must start on a block boundary"
    if end == self._length:
      # special cases
      if end - start == TreeHasher.BLOCK_SIZE:
        # we already rotated
        return self._digests[-1]
      elif end - start < TreeHasher.BLOCK_SIZE:
        # no complete block yet
        return self._current_hash.digest()
      else:
        t = TreeHasher.BLOCK_SIZE
        while start + 2 * t < end:
          t *= 2
        mid_point = start + t
        combo = hashlib.sha256()
        combo.update(self.get_tree_hash(start, mid_point))
        combo.update(self.get_tree_hash(mid_point, end))
        return combo.digest()
    else:
      if ((end - start) % TreeHasher.BLOCK_SIZE) > 0:
        raise Exception("end - start must be multiple of block size")
      start_block = start // TreeHasher.BLOCK_SIZE
      end_block = end // TreeHasher.BLOCK_SIZE
      block_count = end_block - start_block
      if not block_count > 0 or not (block_count & (block_count - 1)) == 0:
        raise Exception("block count must be a power of 2")
      if start_block % block_count > 0:
        raise Exception("start block must be a multiple of block count")
      if block_count >= 2:
        half_block_count = block_count // 2
        mid_point = start + half_block_count * TreeHasher.BLOCK_SIZE
        combo = hashlib.sha256()
        combo.update(self.get_tree_hash(start, mid_point))
        combo.update(self.get_tree_hash(mid_point, end))
        return combo.digest()
      else:
        # single block
        return self._digests[start_block]




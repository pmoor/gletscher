import unittest

import crypto
import hashlib

class MyTestCase(unittest.TestCase):

  def _testBlock(self, i, length=1024 * 1024):
    assert len(i) == 1
    return i[0] * length

  def _hash(self, i, length=1024 * 1024):
    sha = hashlib.sha256()
    sha.update(self._testBlock(i, length))
    return sha.digest()

  def _combine(self, a, b):
    sha = hashlib.sha256()
    sha.update(a)
    sha.update(b)
    return sha.digest()

  def test_single_full_block(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))

    self.assertEqual(
      "bf79be0c21a100565100d16b31deee78ce5391f66c0774405d484ce38b6076e0",
      h.get_tree_hash(0, 1024 * 1024).encode("hex"))

  def test_empty_data(self):
    h = crypto.TreeHasher()

    self.assertEqual(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      h.get_tree_hash(0, 0).encode("hex"))

  def test_empty_data_later_update(self):
    h = crypto.TreeHasher()

    self.assertEqual(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      h.get_tree_hash(0, 0).encode("hex"))

    h.update(self._testBlock("0"))
    self.assertEqual(
      "bf79be0c21a100565100d16b31deee78ce5391f66c0774405d484ce38b6076e0",
      h.get_tree_hash(0, 1024 * 1024).encode("hex"))

  def test_two_full_blocks(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))
    h.update(self._testBlock("1"))

    self.assertEqual(
      self._hash("0"), h.get_tree_hash(0, 1024 * 1024))
    self.assertEqual(
      self._hash("1"), h.get_tree_hash(1024 * 1024, 2 * 1024 * 1024))
    self.assertEqual(
      self._combine(self._hash("0"), self._hash("1")), h.get_tree_hash(0, 2 * 1024 * 1024))
    self.assertEqual(
      "d93d23bf20decc64e3a6a1f004df228b0603fda5ea3db86903f47da493e98c85",
      h.get_tree_hash(0, 2 * 1024 * 1024).encode("hex"))

  def test_three_full_blocks(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))
    h.update(self._testBlock("1"))
    h.update(self._testBlock("2"))

    self.assertEqual(
      self._hash("0"), h.get_tree_hash(0, 1024 * 1024))
    self.assertEqual(
      self._hash("1"), h.get_tree_hash(1024 * 1024, 2 * 1024 * 1024))
    self.assertEqual(
      self._hash("2"), h.get_tree_hash(2 * 1024 * 1024, 3 * 1024 * 1024))
    self.assertEqual(
      self._combine(self._hash("0"), self._hash("1")), h.get_tree_hash(0, 2 * 1024 * 1024))
    self.assertEqual(
      self._combine(
        self._combine(self._hash("0"), self._hash("1")),
        self._hash("2"))
      , h.get_tree_hash(0, 3 * 1024 * 1024))
    self.assertEqual(
      "be55fa01ae74848aeb58cf4213cb8d6d31596dd511a4a82854f7fb3938b5d6be",
      h.get_tree_hash(0, 3 * 1024 * 1024).encode("hex"))

  def test_three_plus_partial(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))
    h.update(self._testBlock("1"))
    h.update(self._testBlock("2"))
    h.update(self._testBlock("3", 1))

    self.assertEqual(
      self._hash("0"), h.get_tree_hash(0, 1024 * 1024))
    self.assertEqual(
      self._hash("1"), h.get_tree_hash(1024 * 1024, 2 * 1024 * 1024))
    self.assertEqual(
      self._hash("2"), h.get_tree_hash(2 * 1024 * 1024, 3 * 1024 * 1024))
    self.assertEqual(
      self._hash("3", 1), h.get_tree_hash(3 * 1024 * 1024, 3 * 1024 * 1024 + 1))
    self.assertEqual(
      self._combine(self._hash("0"), self._hash("1")), h.get_tree_hash(0, 2 * 1024 * 1024))
    self.assertEqual(
      self._combine(
        self._combine(self._hash("0"), self._hash("1")),
        self._combine(self._hash("2"), self._hash("3", 1)),)
      , h.get_tree_hash(0, 3 * 1024 * 1024 + 1))
    self.assertEqual(
      "10d1c8c304aab5431c6c9ebdfb6b10acbd957959504e379f8b433bf80fbe8cc9",
      h.get_tree_hash(0, 3 * 1024 * 1024 + 1).encode("hex"))

  def test_many_plus_partial(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))
    h.update(self._testBlock("1"))
    h.update(self._testBlock("2"))
    h.update(self._testBlock("3"))
    h.update(self._testBlock("4"))
    h.update(self._testBlock("5", 1))

    self.assertEqual(
      self._combine(
        self._combine(
          self._combine(self._hash("0"), self._hash("1")),
          self._combine(self._hash("2"), self._hash("3"))),
        self._combine(
          self._hash("4"),
          self._hash("5", 1))),
      h.get_tree_hash(0, 5 * 1024 * 1024 + 1))

  def test_starting_at_one(self):
    h = crypto.TreeHasher()
    h.update(self._testBlock("0"))
    h.update(self._testBlock("1"))
    h.update(self._testBlock("2"))
    h.update(self._testBlock("3"))
    h.update(self._testBlock("4"))

    self.assertEqual(
      self._combine(
        self._combine(self._hash("1"), self._hash("2")),
        self._hash("3")),
      h.get_tree_hash(1024 * 1024, 4 * 1024 * 1024))

if __name__ == '__main__':
  unittest.main()


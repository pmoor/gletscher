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

import unittest
import hashlib

from gletscher.crypto import TreeHasher, Crypter
from gletscher import hex

class TestCrypter(unittest.TestCase):

    def test_basic_encryption_decryption(self):
        crypter = Crypter(b"\x00" * 32)
        iv, encrypted = crypter.encrypt(b"hello world!", b"\x00" * 16)
        self.assertEqual(b"\x00" * 16, iv)
        self.assertEqual(b"\xb4v\x9cm+\x15z\xd2E\xa6 \x9b", encrypted)

        decrypted = crypter.decrypt(iv, encrypted)
        self.assertEqual(b"hello world!", decrypted)

    def test_standard_test_vector(self):
        """From: NIST 800-38A - F.3.11 CFB8-AES256.Encrypt"""
        crypter = Crypter(hex.h2b("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4"))
        iv, encrypted = crypter.encrypt(
            hex.h2b("6bc1bee22e409f96e93d7e117393172aae2d"), hex.h2b("000102030405060708090a0b0c0d0e0f"))
        self.assertEqual(
            "dc1f1a8520a64db55fcc8ac554844e889700", hex.b2h(encrypted))

        decrypted = crypter.decrypt(iv, encrypted)
        self.assertEqual(
            "6bc1bee22e409f96e93d7e117393172aae2d", hex.b2h(decrypted))

    def test_hmac(self):
        """From: rfc4231 / 4.4"""
        crypter = Crypter(b"\xaa" * 20)
        self.assertEqual(
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
            hex.b2h(crypter.hash(b"\xdd" * 50)))


class TestTreeHasher(unittest.TestCase):
    def _testBlock(self, i, length=1024 * 1024):
        assert len(i) == 1
        return str.encode(i[0]) * length

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
        h = TreeHasher()
        h.update(self._testBlock("0"))

        self.assertEqual(
            "bf79be0c21a100565100d16b31deee78ce5391f66c0774405d484ce38b6076e0",
            hex.b2h(h.get_tree_hash(0, 1024 * 1024)))

    def test_empty_data(self):
        h = TreeHasher()

        self.assertEqual(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            hex.b2h(h.get_tree_hash(0, 0)))

    def test_empty_data_later_update(self):
        h = TreeHasher()

        self.assertEqual(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            hex.b2h(h.get_tree_hash(0, 0)))

        h.update(self._testBlock("0"))
        self.assertEqual(
            "bf79be0c21a100565100d16b31deee78ce5391f66c0774405d484ce38b6076e0",
            hex.b2h(h.get_tree_hash(0, 1024 * 1024)))

    def test_two_full_blocks(self):
        h = TreeHasher()
        h.update(self._testBlock("0"))
        h.update(self._testBlock("1"))

        self.assertEqual(
            self._hash("0"), h.get_tree_hash(0, 1024 * 1024))
        self.assertEqual(
            self._hash("1"), h.get_tree_hash(1024 * 1024, 2 * 1024 * 1024))
        self.assertEqual(
            self._combine(self._hash("0"), self._hash("1")),
            h.get_tree_hash(0, 2 * 1024 * 1024))
        self.assertEqual(
            "d93d23bf20decc64e3a6a1f004df228b0603fda5ea3db86903f47da493e98c85",
            hex.b2h(h.get_tree_hash(0, 2 * 1024 * 1024)))

    def test_three_full_blocks(self):
        h = TreeHasher()
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
            self._combine(self._hash("0"), self._hash("1")),
            h.get_tree_hash(0, 2 * 1024 * 1024))
        self.assertEqual(
            self._combine(
                self._combine(self._hash("0"), self._hash("1")),
                self._hash("2"))
            , h.get_tree_hash(0, 3 * 1024 * 1024))
        self.assertEqual(
            "be55fa01ae74848aeb58cf4213cb8d6d31596dd511a4a82854f7fb3938b5d6be",
            hex.b2h(h.get_tree_hash(0, 3 * 1024 * 1024)))

    def test_three_plus_partial(self):
        h = TreeHasher()
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
            self._hash("3", 1),
            h.get_tree_hash(3 * 1024 * 1024, 3 * 1024 * 1024 + 1))
        self.assertEqual(
            self._combine(self._hash("0"), self._hash("1")),
            h.get_tree_hash(0, 2 * 1024 * 1024))
        self.assertEqual(
            self._combine(
                self._combine(self._hash("0"), self._hash("1")),
                self._combine(self._hash("2"), self._hash("3", 1)), )
            , h.get_tree_hash(0, 3 * 1024 * 1024 + 1))
        self.assertEqual(
            "10d1c8c304aab5431c6c9ebdfb6b10acbd957959504e379f8b433bf80fbe8cc9",
            hex.b2h(h.get_tree_hash(0, 3 * 1024 * 1024 + 1)))

    def test_many_plus_partial(self):
        h = TreeHasher()
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
        h = TreeHasher()
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


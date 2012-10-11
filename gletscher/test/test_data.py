# Copyright 2012 Patrick Moor <patrick@moor.ws>
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

import tempfile
import unittest
from gletscher.crypto import Crypter, TreeHasher
from gletscher.data import DataFile, ReadOnlyDataFile
from gletscher.hex import h2b

SMALLEST_CHUNK_OF_DATA = b"!"
SMALL_CHUNK_OF_DATA = b"a small chunk of data!"

class TestDataFile(unittest.TestCase):
    def test_write_and_read(self):
        crypter = Crypter(b"0" * 32)
        with tempfile.TemporaryDirectory() as dir:
            f = DataFile(dir, 200, crypter)
            self.assertTrue(f.fits(SMALL_CHUNK_OF_DATA))
            self.assertEqual((19, 77), f.add(SMALL_CHUNK_OF_DATA))
            self.assertTrue(f.fits(SMALLEST_CHUNK_OF_DATA))
            self.assertEqual((96, 57), f.add(SMALLEST_CHUNK_OF_DATA))
            self.assertFalse(f.fits(SMALL_CHUNK_OF_DATA))
            tree_hash = f.finalize()

            with open(f.file_name(), "rb") as r:
                hasher = TreeHasher()
                hasher.consume(r)
                self.assertEqual(hasher.get_tree_hash(), tree_hash)

            ro = ReadOnlyDataFile(f.file_name(), crypter)
            self.assertEqual(SMALL_CHUNK_OF_DATA,
                ro.read(19, 77, h2b("11e54425cefb5ba8f55b9c3de9c246da"
                                    "5e99672c60984d792cda8c57c4718ea9")))
            self.assertEqual(SMALLEST_CHUNK_OF_DATA,
                ro.read(96, 57, h2b("619e9f1f44ea07ab76980b54c31296ee"
                                    "1d390a94da8240f142931d202c18b159")))
            ro.close()
            f.delete()


if __name__ == '__main__':
    unittest.main()
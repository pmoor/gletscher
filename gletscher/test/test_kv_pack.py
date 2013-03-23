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

from io import BytesIO
import unittest
import random

from gletscher import kv_pack
from gletscher.crypto import Crypter


class TestKvPack(unittest.TestCase):
    def test_random_write_and_read(self):
        crypter = Crypter(b"0" * 32)

        output = BytesIO()

        mapping = {}
        for i in range(128):
            d = {}
            mapping["%04d" % i] = d
            for j in range(128):
                d[str.encode("%08d" % j)] = str.encode("%x" % random.getrandbits(32))

        lazy_mapping = dict()
        for k, v in mapping.items():
            lazy_mapping[k] = v.items
        kv_pack.kv_pack(output, lazy_mapping, crypter)

        output.seek(0)
        result = {}
        def new_file(name):
            result[name] = {}
            return result[name]
        kv_pack.kv_unpack(output, crypter, new_file, lambda _: False)

        self.assertEqual(mapping, result)


if __name__ == '__main__':
    unittest.main()

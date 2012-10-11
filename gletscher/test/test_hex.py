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

from gletscher.hex import h2b, b2h
import unittest

class TestHex(unittest.TestCase):
    def test_h2b(self):
        self.assertEqual(b"\xaa\xbb\xcc", h2b("aabbcc"))

    def test_b2h(self):
        self.assertEqual("beef", b2h(b"\xbe\xef"))
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

import os
import tempfile
import unittest
from gletscher.config import BackupConfiguration

class TestConfig(unittest.TestCase):
    def test_create_new_and_then_read(self):
        with tempfile.TemporaryDirectory() as dir:
            config = BackupConfiguration.NewEmptyConfiguration(
                dir, prompt_command=lambda *a, **b: "42")
            self.assertEqual("42", config.aws_access_key())
            self.assertEqual(42, config.aws_account_id())
            self.assertEqual("42", config.aws_region())
            self.assertEqual("42", config.aws_secret_access_key())
            self.assertTrue(os.path.isdir(config.catalog_dir_location()))
            self.assertTrue(os.path.isdir(config.tmp_dir_location()))


if __name__ == '__main__':
    unittest.main()
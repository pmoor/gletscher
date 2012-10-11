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

class FileChunker(object):
    def __init__(self, block_size):
        self._block_size = int(block_size)

    def chunk(self, full_path, max_size, base_catalog_entry=None):
        with open(full_path, "rb") as f:
            while True:
                to_read = min(self._block_size, max_size)
                chunk = f.read(to_read)
                if not chunk:
                    break
                max_size -= len(chunk)
                yield chunk

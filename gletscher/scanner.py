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
import stat

class FileScanner(object):
    """Recursively scans a set of directories and files."""
    def __init__(self, files, skip_files=None):
        self._files = set(files)
        self._skip_files_stat = set()
        if skip_files:
            for skip_file in skip_files:
                self._skip_files_stat.add(os.stat(skip_file))

    def __iter__(self):
        for file in self._files:
            assert type(file) == str
            file = os.path.abspath(file)
            file_stat = os.lstat(file)
            if stat.S_ISDIR(file_stat.st_mode):
                for root, dirs, files in os.walk(file, topdown=False):
                    dir_stat = os.lstat(root)
                    if not self._skip(dir_stat):
                      yield root, dir_stat

                    for path in files:
                        full_path = os.path.join(root, path)
                        file_stat = os.lstat(full_path)
                        if not self._skip(file_stat):
                          yield full_path, file_stat
            elif not self._skip(file_stat):
                yield file, file_stat

    def _skip(self, stat):
        for skip_stat in self._skip_files_stat:
            if os.path.samestat(skip_stat, stat):
                return True
        return False

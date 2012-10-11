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

from collections import defaultdict
import math
import os

def human(size):
    if size >= 1024 * 1024:
        return "%0.1f MB" % (size / 1024 / 1024)
    elif size >= 1024:
        return "%0.1f KB" % (size / 1024)
    else:
        return "%0.1f B" % size

sizes = defaultdict(int)

for path, _, files in os.walk("/data/media"):
    for f in files:
        try:
            size = int((os.stat(os.path.join(path, f))).st_size)
            if size:
                l = math.log(size) // math.log(2)
            else:
                l = 0
            if not l in sizes:
                sizes[l] = 1
            else:
                sizes[l] += 1
        except:
            pass

total_files = sum(sizes.values())
cumulative = 0
for k in sorted(sizes.keys()):
    cumulative += sizes[k]
    print("[%s, %s) %d %d %2.4f" % (
        human(2**k), human(2**(k+1)), sizes[k], cumulative,
        100 * cumulative / total_files))

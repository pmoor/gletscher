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

from collections import defaultdict

import logging
from gletscher.aws.aws_helper import AwsHelper
from gletscher import hex


logger = logging.getLogger(__name__)

class CatalogIndexConsistencyChecker(object):

    def __init__(self, catalog, index, missing_archives):
        self._catalog = catalog
        self._index = index
        self._missing_archives = missing_archives

    def find_missing_digests(self):
        paths_missing_data = set()
        paths = digests = size = 0
        for path, entry in self._catalog.entries():
            paths += 1
            if entry.is_regular_file():
                for digest in entry.digests():
                    digests += 1
                    index_entry = self._index.get(digest)
                    if index_entry:
                        size += index_entry.original_length
                        if index_entry.file_tree_hash in self._missing_archives:
                            paths_missing_data.add(path)
                    else:
                        paths_missing_data.add(path)

        print("%d catalog entries with %d digests total (%0.2f MB)" % (
            paths, digests, size / 1024 / 1024))

        if paths_missing_data:
            print("WARNING: %d entries are missing data:" % len(paths_missing_data))
            for path in sorted(paths_missing_data):
                print("  %s" % path.decode("utf8"))

        return paths_missing_data


class IndexArchiveConsistencyChecker(object):

    def __init__(self, backup_id, index, glacier_client, poll_interval):
        self._backup_id = backup_id
        self._index = index
        self._glacier_client = glacier_client
        self._poll_interval = poll_interval

        self._data_archives = set()
        self._catalog_archives = set()
        self._foreign_archives = set()

    def reconcile(self):
        inventory = AwsHelper(self._glacier_client).RetrieveArchive(
            poll_interval=self._poll_interval)

        for archive in inventory:
            if archive.GetBackupId() == self._backup_id:
                if archive.IsDataArchive():
                    self._data_archives.add(archive)
                else:
                    self._catalog_archives.add(archive)
            else:
                self._foreign_archives.add(archive)


        missing_tree_hashes = set()
        available_tree_hashes = set(a.GetTreeHash() for a in self._data_archives)
        digests = 0
        tree_hash_stats = defaultdict(lambda: [0, 0, 0])
        for digest, entry in self._index.entries():
            if not entry.file_tree_hash in available_tree_hashes:
                missing_tree_hashes.add(entry.file_tree_hash)

            digests += 1
            tree_hash_stats[entry.file_tree_hash][0] += 1
            tree_hash_stats[entry.file_tree_hash][1] += entry.original_length / 1024 / 1024
            tree_hash_stats[entry.file_tree_hash][2] += entry.persisted_length / 1024 / 1024

        print("%d digests checked" % digests)
        for k in sorted(tree_hash_stats.keys(), key=lambda d: -tree_hash_stats[d][2]):
            print("  %s: %6d %8.3f %8.3f" % tuple([hex.b2ah(k)] + tree_hash_stats[k]))

        if missing_tree_hashes:
            print("WARNING: some tree hashes are not present in the archive:")
            for tree_hash in sorted(missing_tree_hashes):
                print("  %s" % hex.b2h(tree_hash))

        return missing_tree_hashes

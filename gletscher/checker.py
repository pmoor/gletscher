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

import datetime
import time
import logging
from gletscher.aws.client import GlacierJob
from gletscher import hex


logger = logging.getLogger(__name__)

class CatalogIndexConsistencyChecker(object):

    def __init__(self, catalog, index):
        self._catalog = catalog
        self._index = index

    def assert_all_digests_present(self):
        paths = digests = size = 0
        for path, entry in self._catalog.entries():
            paths += 1
            if entry.is_regular_file():
                for digest in entry.digests():
                    digests += 1
                    index_entry = self._index.get(digest)
                    if index_entry:
                        size += index_entry.original_length
                    else:
                        raise Exception("%s is missing data: %s" % (path, digest))

        logger.info("%d catalog entries with %d digests total (%0.2f MB)",
            paths, digests, size / 1024 / 1024)


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
        most_recent_job = self._find_recent_completed_archive_retrieval_job(
            datetime.timedelta(seconds=30 * 3600))

        connection = self._glacier_client.NewConnection()
        inventory = self._glacier_client._getInventory(connection, most_recent_job.Id())

        for archive in inventory:
            if archive.GetBackupId() == self._backup_id:
                if archive.IsDataArchive():
                    self._data_archives.add(archive)
                else:
                    self._catalog_archives.add(archive)
            else:
                self._foreign_archives.add(archive)


        available_tree_hashes = set(a.GetTreeHash() for a in self._data_archives)
        digests = 0
        tree_hash_stats = defaultdict(lambda: [0, 0, 0])
        for digest, entry in self._index.entries():
            if not entry.file_tree_hash in available_tree_hashes:
                raise Exception("missing tree hash: %s" % entry.file_tree_hash)

            digests += 1
            tree_hash_stats[entry.file_tree_hash][0] += 1
            tree_hash_stats[entry.file_tree_hash][1] += entry.original_length / 1024 / 1024
            tree_hash_stats[entry.file_tree_hash][2] += entry.persisted_length / 1024 / 1024

        print("%d digests verified" % digests)
        for k in sorted(tree_hash_stats.keys(), key=lambda d: -tree_hash_stats[d][2]):
            print(" %s: %6d %8.3f %8.3f" % tuple([hex.b2h(k)] + tree_hash_stats[k]))


    def _find_recent_completed_archive_retrieval_job(self, max_result_age):
        recent_available_jobs = set()
        while not recent_available_jobs:
            connection = self._glacier_client.NewConnection()
            jobs = self._glacier_client._listJobs(connection)

            recent_pending_jobs = set()
            for job in jobs:
                if job.IsInventoryRetrieval():
                    if job.CompletedSuccessfully() and job.ResultAge() <\
                       max_result_age:
                        recent_available_jobs.add(job)
                    elif job.IsPending() and job.CreationAge() < max_result_age:
                        recent_pending_jobs.add(job)

            connection.close()
            if not recent_available_jobs and not recent_pending_jobs:
                logger.info(
                    "no recent job available or pending - starting a new one")
                self._glacier_client._initiateInventoryRetrieval(connection)
                time.sleep(self._poll_interval)
            elif not recent_available_jobs:
                logger.info(
                    "no recent job available - waiting for pending to complete")
                time.sleep(self._poll_interval)
        most_recent_job = min(recent_available_jobs, key=GlacierJob.ResultAge)
        return most_recent_job
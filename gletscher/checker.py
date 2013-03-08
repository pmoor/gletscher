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

import datetime
import json
import time
import logging
import uuid
from gletscher.aws import GlacierJob
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

    def __init__(self, backup_id, index, glacier_client):
        self._backup_id = backup_id
        self._index = index
        self._glacier_client = glacier_client

    def reconcile(self):
        most_recent_job = self._find_recent_completed_archive_retrieval_job(
            datetime.timedelta(seconds=30 * 3600))

        connection = self._glacier_client.NewConnection()
        inventory = self._glacier_client._getInventory(connection, most_recent_job.Id())

        available_data = set()
        for archive in inventory:
            if archive.GetBackupId() == self._backup_id and archive.IsDataArchive():
                available_data.add(archive.GetTreeHash())

        for digest, entry in self._index.entries():
            if not entry.file_tree_hash in available_data:
                raise Exception("missing tree hash: %s" % entry.file_tree_hash)


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
                time.sleep(300)
            elif not recent_available_jobs:
                logger.info(
                    "no recent job available - waiting for pending to complete")
                time.sleep(300)
        most_recent_job = min(recent_available_jobs, key=GlacierJob.ResultAge)
        return most_recent_job
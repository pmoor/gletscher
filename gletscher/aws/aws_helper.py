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
import logging
import time

from gletscher.aws.client import GlacierJob

logger = logging.getLogger(__name__)

class AwsHelper(object):

    def __init__(self, glacier_client):
        self._glacier_client = glacier_client

    def RetrieveArchive(self,
            max_age=datetime.timedelta(seconds=30 * 3600), poll_interval=900):

        recent_available_jobs = set()
        while not recent_available_jobs:
            connection = self._glacier_client.NewConnection()
            jobs = self._glacier_client._listJobs(connection)

            recent_pending_jobs = set()
            for job in jobs:
                if job.IsInventoryRetrieval():
                    if job.CompletedSuccessfully() and job.ResultAge() < max_age:
                        recent_available_jobs.add(job)
                    elif job.IsPending() and job.CreationAge() < max_age:
                        recent_pending_jobs.add(job)

            connection.close()
            if not recent_available_jobs and not recent_pending_jobs:
                logger.info(
                    "no recent job available or pending - starting a new one")
                self._glacier_client._initiateInventoryRetrieval(connection)
                time.sleep(poll_interval)
            elif not recent_available_jobs:
                logger.info(
                    "no recent job available - waiting for pending to complete")
                time.sleep(poll_interval)
        most_recent_job = min(recent_available_jobs, key=GlacierJob.ResultAge)

        connection = self._glacier_client.NewConnection()
        return self._glacier_client._getInventory(connection, most_recent_job.Id())
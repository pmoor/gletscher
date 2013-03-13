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

from gletscher.aws.client import GlacierClient
from gletscher.config import BackupConfiguration

def glacier_list_jobs_command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    glacier_client = GlacierClient.FromConfig(config)
    connection = glacier_client.NewConnection()

    jobs = glacier_client._listJobs(connection)
    print("%d jobs found" % len(jobs))
    for job in jobs:
      print(job)
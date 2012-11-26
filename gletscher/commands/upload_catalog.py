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

from datetime import datetime
import os
from gletscher.aws import GlacierClient
from gletscher.catalog import Catalog
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.index import Index
import logging
from gletscher.kv_pack import kv_pack

logger = logging.getLogger(__name__)

def upload_catalog_command(args):
    assert not args.catalog.startswith("_")
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())
    glacier_client = GlacierClient.FromConfig(config)

    index = Index(config.index_dir_location())
    catalog = Catalog(
        config.catalog_dir_location(), args.catalog, truncate=False)

    file_path = os.path.join(
        config.tmp_dir_location(),
        "%s-%s.ci" % (
        args.catalog, datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")))

    kv_pack(file_path, {1: catalog.raw_entries(), 2: index.raw_entries()}, crypter)

    catalog.close()
    index.close()

    glacier_client.upload_file(
        file_path,
        description={
            "backup": str(config.uuid()),
            "type": "catalog/index",
            "catalog": args.catalog})

    os.unlink(file_path)

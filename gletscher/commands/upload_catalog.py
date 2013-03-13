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
import logging
import dbm.gnu

from gletscher.aws.client import GlacierClient
from gletscher.catalog import Catalog
from gletscher.checker import CatalogIndexConsistencyChecker
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.index import Index
from gletscher.kv_pack import kv_pack


logger = logging.getLogger(__name__)

def register(subparsers):
    upload_catalog_parser = subparsers.add_parser(
        "upload_catalog", help="uploads a catalog/index")
    upload_catalog_parser.add_argument(
        "--catalog", help="catalog to upload", required=True, default="default")
    upload_catalog_parser.set_defaults(fn=command)

def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())
    glacier_client = GlacierClient.FromConfig(config)

    index_db = dbm.gnu.open(config.index_file_location(), "r")
    index = Index(index_db)

    catalog_db = dbm.gnu.open(config.catalog_location(args.catalog), "r")
    catalog = Catalog(catalog_db)

    checker = CatalogIndexConsistencyChecker(catalog, index)
    checker.assert_all_digests_present()

    file_path = os.path.join(
        config.tmp_dir_location(),
        "%s-%s.ci" % (
        args.catalog, datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")))

    kv_pack(file_path, {1: catalog_db, 2: index_db}, crypter)

    catalog.close()
    index.close()

    glacier_client.upload_file(
        file_path,
        description={
            "backup": str(config.uuid()),
            "type": "catalog/index",
            "catalog": args.catalog})

    os.unlink(file_path)
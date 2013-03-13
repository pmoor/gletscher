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

import logging
import dbm

from gletscher.aws.client import GlacierClient
from gletscher.catalog import Catalog
from gletscher.checker import IndexArchiveConsistencyChecker, CatalogIndexConsistencyChecker
from gletscher.config import BackupConfiguration
from gletscher.index import Index


logger = logging.getLogger(__name__)

def register(subparsers):
    reconile_parser = subparsers.add_parser(
        "reconcile", help="reconciles the index")
    reconile_parser.add_argument(
        "catalogs", metavar="catalog", nargs="+",
        help="catalogs to check against index")
    reconile_parser.set_defaults(fn=command)

def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    glacier_client = GlacierClient.FromConfig(config)

    index_db = dbm.gnu.open(config.index_file_location(), "r")
    index = Index(index_db)

    for name in args.catalogs:
        print("reconciling catalog \"%s\" against index..." % name)
        catalog_db = dbm.gnu.open(config.catalog_location(name), "r")
        catalog = Catalog(catalog_db)

        checker = CatalogIndexConsistencyChecker(catalog, index)
        checker.assert_all_digests_present()
        catalog.close()
        print("OK.")

    print("reconciling index against AWS Glacier archive...")
    checker = IndexArchiveConsistencyChecker(config.uuid(), index, glacier_client)
    checker.reconcile()
    print("OK.")
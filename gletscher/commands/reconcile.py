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

import logging
import dbm.gnu
import os
import re

from gletscher.aws.client import GlacierClient
from gletscher.catalog import Catalog
from gletscher.checker import IndexArchiveConsistencyChecker, CatalogIndexConsistencyChecker
from gletscher.config import BackupConfiguration
from gletscher.index import Index

CATALOG_NAME_RE = re.compile(r"(.+)\.catalog")

logger = logging.getLogger(__name__)

def register(subparsers):
    reconcile_parser = subparsers.add_parser(
        "reconcile", help="reconciles the index")
    reconcile_parser.add_argument(
        "--poll_interval", type=float, default=900.0)
    reconcile_parser.set_defaults(fn=command)

def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    glacier_client = GlacierClient.FromConfig(config)

    index_db = dbm.gnu.open(config.index_file_location(), "r")
    index = Index(index_db)

    print("reconciling index against AWS Glacier archive...")
    checker = IndexArchiveConsistencyChecker(
        config.uuid(), index, glacier_client, args.poll_interval)
    missing_archives = checker.reconcile()
    has_missing_paths = False

    for catalog_name in _find_all_catalogs(config):
        print("reconciling catalog \"%s\" against index..." % catalog_name)
        catalog_db = dbm.gnu.open(config.catalog_location(catalog_name), "r")
        catalog = Catalog(catalog_db)

        checker = CatalogIndexConsistencyChecker(catalog, index, missing_archives)
        if checker.find_missing_digests():
            has_missing_paths = True
        catalog.close()

    if has_missing_paths or missing_archives:
        print("FAILURE: data is missing")
    else:
        print("OK")

def _find_all_catalogs(config):
    files = [f for f in os.listdir(config.catalog_dir_location())]
    return sorted([CATALOG_NAME_RE.match(c).group(1) for c in files if CATALOG_NAME_RE.match(c)])

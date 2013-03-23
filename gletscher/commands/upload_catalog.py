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

import json
import os
import logging
import dbm.gnu
import re

from gletscher.aws.client import GlacierClient
from gletscher.aws.streaming import StreamingUploader
from gletscher.catalog import Catalog
from gletscher.checker import CatalogIndexConsistencyChecker
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.index import Index
from gletscher.kv_pack import kv_pack

CATALOG_NAME_RE = re.compile(r"(.+)\.catalog")

logger = logging.getLogger(__name__)

def register(subparsers):
    upload_catalog_parser = subparsers.add_parser(
        "upload_catalog", help="uploads a catalog/index")
    upload_catalog_parser.set_defaults(fn=command)

def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    files = {}

    index_db = dbm.gnu.open(config.index_file_location(), "r")
    index = Index(index_db)
    files["index"] = _kv_generator(index_db)

    for catalog_name in _find_all_catalogs(config):
        catalog_db = dbm.gnu.open(config.catalog_location(catalog_name), "r")
        catalog = Catalog(catalog_db)

        print("checking catalog \"%s\"" % catalog_name)
        checker = CatalogIndexConsistencyChecker(catalog, index)
        checker.assert_all_digests_present()

        files["%s.catalog" % catalog_name] = _kv_generator(catalog_db)

    crypter = Crypter(config.secret_key())
    glacier_client = GlacierClient.FromConfig(config)
    streaming_uploader = StreamingUploader(glacier_client)

    print("uploading all catalogs")
    pending_upload = streaming_uploader.new_upload(
        json.dumps({"backup": str(config.uuid()), "type": "catalog"}))

    kv_pack(pending_upload, files, crypter)
    pending_upload.finish()

    print("done")

def _find_all_catalogs(config):
    files = [f for f in os.listdir(config.catalog_dir_location())]
    return sorted([CATALOG_NAME_RE.match(c).group(1) for c in files if CATALOG_NAME_RE.match(c)])

def _kv_generator(db):
    def gen():
        k = db.firstkey()
        while k is not None:
            yield k, db[k]
            k = db.nextkey(k)
    return gen

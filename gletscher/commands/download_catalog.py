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

import dbm
import os
from gletscher.catalog import Catalog
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.index import Index
from gletscher.kv_pack import kv_unpack

def download_catalog_command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())

    tmp_catalog_path = os.path.join(config.tmp_dir_location(), "tmp-catalog")
    tmp_catalog = dbm.gnu.open(tmp_catalog_path, "nf")

    tmp_index_path = os.path.join(config.tmp_dir_location(), "tmp-index")
    tmp_index = dbm.gnu.open(tmp_index_path, "nf")

    kv_unpack("/Users/pmoor/Repositories/Gletscher/indexcatalog.bin",
        {1: tmp_catalog, 2: tmp_index}, crypter)

    index = Index(config.index_dir_location())
    index.MergeWith(tmp_index)
    index.close()

    catalog = Catalog(config.catalog_location("imported"), truncate=True)
    catalog.MergeWith(tmp_catalog)
    catalog.close()

    tmp_index.close()
    tmp_catalog.close()
    os.unlink(tmp_index_path)
    os.unlink(tmp_catalog_path)

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

import bz2
from datetime import datetime
import os
import struct
from gletscher.aws import GlacierClient
from gletscher.catalog import Catalog
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.index import Index
import logging

logger = logging.getLogger(__name__)

def upload_catalog_command(args):
    assert not args.catalog.startswith("_")
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())
    glacier_client = GlacierClient.FromConfig(config)

    index = Index(config.index_dir_location())
    catalog = Catalog(config.catalog_dir_location(), args.catalog,
                      truncate=False)

    file_path = os.path.join(
        config.tmp_dir_location(),
        "%s-%s.ci" % (
        args.catalog, datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")))
    f = open(file_path, "w")

    iv, cipher = crypter.new_cipher()
    f.write(iv)

    compressor = bz2.BZ2Compressor()

    count = 0
    for k, v in catalog.raw_entries():
        f.write(cipher.encrypt(
            compressor.compress(struct.pack(">LL", len(k), len(v)))))
        f.write(cipher.encrypt(compressor.compress(k)))
        f.write(cipher.encrypt(compressor.compress(v)))
        count += 1

    logger.info("wrote %d catalog entries", count)

    count = 0
    for k, v in index.raw_entries():
        f.write(cipher.encrypt(
            compressor.compress(struct.pack(">LL", len(k), len(v)))))
        f.write(cipher.encrypt(compressor.compress(k)))
        f.write(cipher.encrypt(compressor.compress(v)))
        count += 1

    logger.info("wrote %d index entries", count)

    f.write(cipher.encrypt(compressor.flush()))
    f.close()

    index.close()
    catalog.close()

    glacier_client.upload_file(
        file_path, {"backup": str(config.uuid()), "type": "catalog/index",
                    "catalog": args.catalog})

    os.unlink(file_path)
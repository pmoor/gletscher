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
from datetime import datetime

import dbm.gnu
import json
import os
import stat
import tempfile
import logging

from gletscher.aws.client import GlacierClient
from gletscher.aws.streaming import StreamingUploader
from gletscher.catalog import Catalog
from gletscher.chunker import FileChunker
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.chunk_streamer import ChunkStreamer
from gletscher.index import Index
from gletscher.kv_pack import kv_pack
from gletscher.scanner import FileScanner

logger = logging.getLogger(__name__)

def register(subparsers):
    backup_parser = subparsers.add_parser(
        "backup", help="start backing-up some directories")
    backup_parser.add_argument(
        "--catalog", help="catalog name to use", required=False, default="default")
    backup_parser.add_argument(
        "--exclude", help="files or directories to exclude", action="append", default=[])
    backup_parser.add_argument(
        "files", metavar="file", nargs="+",
        help="a set of files and directories to be backed-up")
    backup_parser.set_defaults(fn=command)


def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())
    chunker = FileChunker(32 * 1024 * 1024)

    main_index = Index(dbm.gnu.open(config.index_file_location(), "cf", 0o600))
    global_catalog = Catalog(dbm.gnu.open(config.global_catalog_location(), "cf", 0o600))

    tmp_catalog_name = tempfile.mktemp(prefix='gletscher-tmp', dir=config.tmp_dir_location())
    tmp_catalog = Catalog(dbm.gnu.open(tmp_catalog_name, "nf", 0o600))

    glacier_client = GlacierClient.FromConfig(config)
    streaming_uploader = StreamingUploader(glacier_client)
    data_streamer = ChunkStreamer(main_index, streaming_uploader, crypter, config.uuid())

    scanner = FileScanner(
        args.files, skip_files=args.exclude + [config.config_dir_location()])
    for full_path, file_stat in scanner:
        if stat.S_ISREG(file_stat.st_mode):
            base_entry = global_catalog.find(full_path)
            if base_entry and not base_entry.has_changed(file_stat):
                # make sure we still have all the pieces in the index
                all_digests_in_index = True
                for digest in base_entry.digests():
                    if not main_index.contains(digest):
                        all_digests_in_index = False
                        break

                if all_digests_in_index:
                    # we've got this file covered
                    tmp_catalog.transfer(full_path, base_entry)
                    continue
            else:
                logger.debug("the catalog contains no entry for %s", full_path)

            digests = []
            total_length = 0
            for chunk in chunker.chunk(full_path, file_stat.st_size, base_entry):
                digest = crypter.hash(chunk)
                if not main_index.contains(digest):
                    data_streamer.upload(digest, chunk)

                digests.append(digest)
                total_length += len(chunk)

            tmp_catalog.add_file(
                full_path, file_stat, digests, total_length)
            global_catalog.add_file(
                full_path, file_stat, digests, total_length)
        else:
            tmp_catalog.add(full_path, file_stat)
            global_catalog.add(full_path, file_stat)

    data_streamer.finish()
    global_catalog.close()

    # upload the catalog
    pending_upload = streaming_uploader.new_upload(
        json.dumps({
            "backup": str(config.uuid()),
            "type": "catalog",
            "name": args.catalog}))
    files = {
        "index": _kv_generator(main_index._db),
        "catalog": _kv_generator(tmp_catalog._db),
    }
    kv_pack(pending_upload, files, crypter)
    pending_upload.finish()
    streaming_uploader.finish()

    main_index.close()
    tmp_catalog.close()

    full_catalog_name = "%s-%s" % (args.catalog, datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    os.rename(tmp_catalog_name, config.catalog_location(full_catalog_name))


def _kv_generator(db):
    def gen():
        k = db.firstkey()
        while k is not None:
            yield k, db[k]
            k = db.nextkey(k)
    return gen

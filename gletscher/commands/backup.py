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

import dbm
import os
import stat
import tempfile
import logging

from gletscher.aws import GlacierClient
from gletscher.catalog import Catalog
from gletscher.chunker import FileChunker
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.data import DataFileWriter
from gletscher.index import Index, TemporaryIndex
from gletscher.scanner import FileScanner
from gletscher import hex


logger = logging.getLogger(__name__)

def backup_command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    crypter = Crypter(config.secret_key())
    chunker = FileChunker(config.max_chunk_size())

    main_index = Index(dbm.gnu.open(config.index_file_location(), "cf", 0o600))
    global_catalog = Catalog(dbm.gnu.open(config.global_catalog_location(), "cf", 0o600))

    tmp_catalog_name = tempfile.mktemp(prefix='glacier-tmp', dir=config.tmp_dir_location())
    tmp_catalog = Catalog(dbm.gnu.open(tmp_catalog_name, "nf", 0o600))

    tmp_index_name = tempfile.mktemp(prefix='glacier-tmp', dir=config.tmp_dir_location())
    tmp_index = TemporaryIndex(dbm.gnu.open(tmp_index_name, "nf", 0o600))

    tmp_data_name = tempfile.mktemp(prefix='glacier-tmp', dir=config.tmp_dir_location())
    tmp_data = DataFileWriter(open(tmp_data_name, "wb"), crypter)

    scanner = FileScanner(args.files, skip_files=[config.config_dir_location()])
    for full_path, file_stat in scanner:
        if stat.S_ISREG(file_stat.st_mode):
            base_entry = global_catalog.find(full_path)
            if base_entry and not base_entry.has_changed(file_stat):
                # make sure we still have all the pieces in the index
                all_digests_in_index = True
                for digest in base_entry.digests():
                    if not main_index.contains(digest) and not tmp_index.contains(digest):
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
            for chunk in chunker.chunk(
                    full_path, file_stat.st_size, base_entry):
                chunk_length = len(chunk)
                digest = crypter.hash(chunk)
                if not main_index.contains(digest) and not tmp_index.contains(digest):
                    logger.debug("new chunk: %s", hex.b2h(digest))

                    if tmp_data.bytes_written() + chunk_length > config.max_data_file_size():
                        # data file would grow too big, finish the current file
                        tree_hasher = tmp_data.close()
                        tmp_index.close()

                        _upload_file_and_merge_index(tree_hasher, tmp_data_name, tmp_index_name, main_index, config)

                        tmp_index_name = tempfile.mktemp(prefix='glacier-tmp', dir=config.tmp_dir_location())
                        tmp_index = TemporaryIndex(dbm.gnu.open(tmp_index_name, "nf", 0o600))

                        tmp_data_name = tempfile.mktemp(prefix='glacier-tmp', dir=config.tmp_dir_location())
                        tmp_data = DataFileWriter(open(tmp_data_name, "wb"), crypter)

                    offset, length = tmp_data.append_chunk(chunk)
                    tmp_index.add(digest, offset, length, chunk_length)

                digests.append(digest)
                total_length += chunk_length

            tmp_catalog.add_file(
                full_path, file_stat, digests, total_length)
            global_catalog.add_file(
                full_path, file_stat, digests, total_length)
        else:
            tmp_catalog.add(full_path, file_stat)
            global_catalog.add(full_path, file_stat)

    tree_hasher = tmp_data.close()
    tmp_index.close()

    if tmp_data.chunks_written():
        _upload_file_and_merge_index(tree_hasher, tmp_data_name, tmp_index_name, main_index, config)
    else:
        os.unlink(tmp_data_name)
        os.unlink(tmp_index_name)

    main_index.close()
    global_catalog.close()

    tmp_catalog.close()

    os.rename(
        tmp_catalog_name,
        os.path.join(config.catalog_dir_location(), "%s.catalog" % args.catalog))


def _upload_file_and_merge_index(tree_hasher, tmp_data_name, tmp_index_name, main_index, config):
    tree_hash = tree_hasher.get_tree_hash()

    glacier_client = GlacierClient.FromConfig(config)
    glacier_client.upload_file(
        tmp_data_name,
        tree_hasher,
        description={
            "backup": str(config.uuid()),
            "type": "data",
            "tree-hash": hex.b2h(tree_hash),
        })

    tmp_index = TemporaryIndex(dbm.gnu.open(tmp_index_name, "r"))
    main_index.merge_temporary_index(tmp_index, tree_hash)
    tmp_index.close()

    os.unlink(tmp_data_name)
    os.unlink(tmp_index_name)
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

import os
import re
import time
import logging
import dbm.gnu
from gletscher.aws.aws_helper import AwsHelper

from gletscher.aws.client import GlacierClient
from gletscher.catalog import Catalog
from gletscher.config import BackupConfiguration
from gletscher.index import Index
from gletscher import hex, crypto

logger = logging.getLogger(__name__)

def register(subparsers):
    restore_parser = subparsers.add_parser(
        "restore", help="looks for files in a catalog")
    restore_parser.add_argument(
        "--catalog", help="catalog to search", required=True)
    restore_parser.add_argument(
        "reg_exps", help="regular expressions to match against", nargs="+")
    restore_parser.set_defaults(fn=command)


def command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    patterns = [re.compile(x.encode()) for x in args.reg_exps]

    catalog = Catalog(
        dbm.gnu.open(config.catalog_location(args.catalog), "r"))
    index = Index(
        dbm.gnu.open(config.index_file_location(), "r"))

    files_needed = {}

    for full_path, entry in catalog.match(patterns):
        if entry.is_directory():
            type = "dir"
            additional = ""
        elif entry.is_link():
            type = "link"
            additional = " -> " + entry.target().decode()
        elif entry.is_regular_file():
            type = "file"
            additional = " %0.3f MB" % (entry._size / 1024 / 1024)
        else:
            raise Exception("odd type")
        print("  %s [%s]%s" % (full_path.decode(), type, additional))

        if entry.is_regular_file():
            for digest in entry.digests():
                index_entry = index.get(digest)

                if not index_entry.file_tree_hash in files_needed:
                    files_needed[index_entry.file_tree_hash] = set()
                first_mb_block = index_entry.offset // (1024 * 1024)
                last_mb_block = (index_entry.offset + index_entry.persisted_length) // (1024 * 1024)
                for i in range(first_mb_block, last_mb_block + 1):
                    files_needed[index_entry.file_tree_hash].add(i)

    print()
    print("files needed for restore:")
    for i in sorted(files_needed.keys()):
        print("  %s (~%d MB)" % (hex.b2ah(i), len(files_needed[i])))

    glacier_client = GlacierClient.FromConfig(config)

    inventory = AwsHelper(glacier_client).RetrieveArchive()
    all_archive_ids = {}
    for archive in inventory:
        if archive.GetBackupId() == config.uuid():
            if archive.IsDataArchive():
                all_archive_ids[archive.GetTreeHash()] = archive.GetId()

    archive_ids = {}
    for file_needed in files_needed:
        archive_ids[all_archive_ids[file_needed]] = file_needed

    DownloadArchives(glacier_client, config.tmp_dir_location(), archive_ids)

    crypter = crypto.Crypter(config.secret_key())
    for full_path, entry in catalog.match(patterns):
        if entry.is_regular_file():
            path = os.path.join(b"/tmp/restore", full_path[1:])
            d = os.path.dirname(path)
            if not os.path.isdir(d):
                os.makedirs(d, mode=0o700)
            logger.info("restoring %s to %s", full_path, path)
            with open(path, "wb") as output:
                for digest in entry.digests():
                    index_entry = index.get(digest)
                    path = os.path.join(
                        config.tmp_dir_location(),
                        "%s.remote" % hex.b2h(index_entry.file_tree_hash))
                    f = open(path, "rb")
                    f.seek(index_entry.offset)
                    data = f.read(index_entry.persisted_length)
                    chunk = crypter.DecryptChunk(
                        index_entry.storage_version, digest, data)
                    output.write(chunk)
        elif entry.is_directory():
            path = os.path.join(b"/tmp/restore", full_path[1:])
            if not os.path.isdir(path):
                os.makedirs(path, mode=0o700)


def DownloadArchives(glacier_client, dir, archives):
    finished_archives = set()

    for archive_id, tree_hash in archives.items():
        path = os.path.join(dir, "%s.remote" % hex.b2h(tree_hash))
        if os.path.isfile(path):
            finished_archives.add(archive_id)

    while finished_archives != archives.keys():
        connection = glacier_client.NewConnection()
        jobs = glacier_client._listJobs(connection)
        print("found jobs:", jobs)

        pending_archives = set()
        for job in jobs:
            if job.IsArchiveRetrieval() and not job.GetArchiveId() in \
                    finished_archives:
                if job.CompletedSuccessfully() and job.GetArchiveId() in \
                        archives:
                    if job.GetTreeHash() == archives[job.GetArchiveId()]:
                        # download file
                        path = os.path.join(
                            dir, "%s.remote" % hex.b2h(job.GetTreeHash()))
                        with open(path, "wb") as f:
                            f.write(glacier_client._getJobOutput(
                                connection, job.Id()))
                        finished_archives.add(job.GetArchiveId())
                elif job.IsPending() and job.GetArchiveId() in archives:
                    if job.GetTreeHash() == archives[job.GetArchiveId()]:
                        pending_archives.add(job.GetArchiveId())

        missing_archives = archives.keys() - finished_archives - pending_archives
        for missing_archive in missing_archives:
            glacier_client._initiateArchiveRetrieval(connection, missing_archive)

        print("needed: ", set(archives.keys()))
        print("finished: ", finished_archives)
        print("pending: ", pending_archives)
        print("missing: ", missing_archives)

        if finished_archives != archives.keys():
            time.sleep(5)

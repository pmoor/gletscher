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
from gletscher.aws import GlacierClient
from gletscher.config import BackupConfiguration
from gletscher.index import Index
import logging
from gletscher import hex, crypto

logger = logging.getLogger(__name__)

def repair_command(args):
    config = BackupConfiguration.LoadFromFile(args.config)

    glacier_client = GlacierClient.FromConfig(config)

    index = Index(config.index_dir_location(), consistency_check=False)

    file_entries = index.FindAllFileEntries()

    for file_id in sorted(file_entries.keys()):
        entry = file_entries[file_id]

        if not entry:
            print(
                "data file id [%d] is referenced by index entries, "
                "but no entry for the file is found." % file_id)
            answer = None
            while True:
                answer = input(
                    "remove all index references to this file? [y|n] ")\
                    .strip().lower()
                if answer == "y" or answer == "n":
                    break
            if answer == "y":
                print("Removing all index entries for %d" % file_id)
                index.RemoveAllEntriesForFile(file_id)
                print("All entries removed.")
            else:
                print(
                    "Not removing stale index entries. The index will remain "
                    "corrupt.")

        elif not entry.archive_id:
            data_file = os.path.join(config.tmp_dir_location(),
                                     "%s.data" % hex.b2h(entry.tree_hash))
            if not os.path.isfile(data_file):
                print("The data file for [%d] does no longer exist." % file_id)
                print(
                    "You can either remove all index entries pointing to it, "
                    "or you can ")
                print("attempt a reconciliation.")
                answer = None
                while True:
                    answer = input(
                        "Remove all index references to this file? [y|n] ")\
                    .strip().lower()
                    if answer == "y" or answer == "n":
                        break
                if answer == "y":
                    print("Removing all index entries for %d" % file_id)
                    index.RemoveAllEntriesForFile(file_id)
                    print("All entries removed.")
                else:
                    print("Not removing stale index entries.")

            else:
                # check tree hash
                tree_hasher = crypto.TreeHasher()
                with open(data_file, "rb") as f:
                    tree_hasher.consume(f)

                computed_tree_hash = tree_hasher.get_tree_hash()
                logger.info("computed tree hash %s",
                            hex.b2h(computed_tree_hash))
                expected_tree_hash = entry.tree_hash
                logger.info("expected tree hash %s",
                            hex.b2h(expected_tree_hash))

                pending_upload = glacier_client.find_pending_upload(
                    config.uuid(), expected_tree_hash)

                if not pending_upload:
                    archive_id, tree_hash = glacier_client.upload_file(
                        data_file, tree_hasher,
                        description={"backup": str(config.uuid()),
                                     "type": "data",
                                     "tree-hash": hex.b2h(expected_tree_hash)})
                    index.finalize_data_file(file_id, tree_hash, archive_id)
                    os.remove(data_file)
                else:
                    archive_id, tree_hash = glacier_client.upload_file(
                        data_file, tree_hasher, pending_upload=pending_upload)
                    index.finalize_data_file(file_id, tree_hash, archive_id)
                    os.remove(data_file)

        else:
            logger.info("everything looks fine for id %d: %s (%s)",
                        file_id, hex.b2h(entry.tree_hash), entry.archive_id)
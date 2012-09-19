import os
from aws import GlacierClient
from config import BackupConfiguration
import crypto
from index import Index
import logging

logger = logging.getLogger(__name__)

def repair_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  glacier_client = GlacierClient(
    config.aws_region(),
    config.aws_account_id(),
    config.vault_name(),
    config.aws_access_key(),
    config.aws_secret_access_key(),
    config.upload_chunk_size())

  index = Index(config.index_dir_location(), consistency_check=False)

  file_entries = index.FindAllFileEntries()

  for file_id in sorted(file_entries.keys()):
    entry = file_entries[file_id]

    if not entry:
      print "data file id [%d] is referenced by index entries, but no entry for the file is found." % file_id
      answer = None
      while True:
        answer = raw_input("remove all index references to this file? [y|n] ").strip().lower()
        if answer == "y" or answer == "n":
          break
      if answer == "y":
        print "Removing all index entries for %d" % file_id
        index.RemoveAllEntriesForFile(file_id)
        print "All entries removed."
      else:
        print "Not removing stale index entries. The index will remain corrupt."

    elif not entry.archive_id:
      data_file = os.path.join(config.tmp_dir_location(), "%s.data" % entry.tree_hash.encode("hex"))
      if not os.path.isfile(data_file):
        print "The data file for [%d] does no longer exist." % file_id
        print "You can either remove all index entries pointing to it, or you can "
        print "attempt a reconciliation."
        answer = None
        while True:
          answer = raw_input("Remove all index references to this file? [y|n] ").strip().lower()
          if answer == "y" or answer == "n":
            break
        if answer == "y":
          print "Removing all index entries for %d" % file_id
          index.RemoveAllEntriesForFile(file_id)
          print "All entries removed."
        else:
          print "Not removing stale index entries."

      else:
        # check tree hash
        tree_hasher = crypto.TreeHasher()
        with open(data_file) as f:
          tree_hasher.consume(f)

        computed_tree_hash = tree_hasher.get_tree_hash()
        logger.info("computed tree hash %s", computed_tree_hash.encode("hex"))
        expected_tree_hash = entry.tree_hash
        logger.info("expected tree hash %s", expected_tree_hash.encode("hex"))

        pending_upload = glacier_client.find_pending_upload(
          config.uuid(), expected_tree_hash)

        if not pending_upload:
          archive_id, tree_hash = glacier_client.upload_file(
            data_file, tree_hasher, description={"backup": str(config.uuid()), "type": "data", "tree-hash": "%s" % expected_tree_hash.encode("hex")})
          index.finalize_data_file(file_id, tree_hash, archive_id)
          os.remove(data_file)
        else:
          archive_id, tree_hash = glacier_client.upload_file(
            data_file, tree_hasher, pending_upload=pending_upload)
          index.finalize_data_file(file_id, tree_hash, archive_id)
          os.remove(data_file)

    else:
      logger.info("everything looks fine for id %d: %s (%s)",
          file_id, entry.tree_hash.encode("hex"), entry.archive_id)
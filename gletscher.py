from datetime import datetime
import httplib
import json
import os
import struct
import stat
import logging
import argparse
import sys
import bz2
import re
import crypto

logging.basicConfig(level=logging.DEBUG)

from crypto import Crypter
from index import Index
from catalog import Catalog
from data import DataFile
from chunker import FileChunker
from scanner import FileScanner
from aws import GlacierClient
from config import BackupConfiguration

logger = logging.getLogger(__name__)

def new_command(args):
  if os.path.isdir(args.config):
    print "the configuration directory must not exist yet"
    sys.exit(1)
  os.mkdir(args.config)
  assert os.path.isdir(args.config)

  config = BackupConfiguration.NewEmptyConfiguration(args.config)

  os.mkdir(config.index_dir_location())
  os.mkdir(config.tmp_dir_location())
  os.mkdir(config.catalog_dir_location())

  Index.CreateEmpty(config.index_dir_location())


def backup_command(args):
  assert not args.catalog.startswith("_")
  config = BackupConfiguration.LoadFromFile(args.config)

  assert os.path.isdir(config.catalog_dir_location()), "catalog directory does not exist: " + config.catalog_dir_location()
  assert os.path.isdir(config.tmp_dir_location()), "temp directory does not exist: " + config.tmp_dir_location()

  crypter = Crypter(config.secret_key())
  glacier_client = GlacierClient(
      config.aws_region(),
      config.aws_account_id(),
      config.vault_name(),
      config.aws_access_key(),
      config.aws_secret_access_key(),
      config.upload_chunk_size())
  chunker = FileChunker(config.max_chunk_size())

  index = Index(config.index_dir_location())
  global_catalog = Catalog(config.catalog_dir_location(), "_global")
  catalog = Catalog(config.catalog_dir_location(), args.catalog, truncate=True)

  data_file = None

  scanner = FileScanner([x.decode("utf8") for x in args.dir])
  for full_path, file_stat in scanner:
    if stat.S_ISREG(file_stat.st_mode):
      digests = []

      base_entry = global_catalog.find(full_path)
      if base_entry and not base_entry.has_changed(file_stat):
        # make sure we still have all the pieces in the index
        all_digests_in_index = True
        for digest in base_entry.digests():
          if not index.find(digest):
            all_digests_in_index = False
            break

        if all_digests_in_index:
          # we've got this file covered
          catalog.transfer(full_path, base_entry)
          continue
      else:
        logger.debug("the catalog contains no entry for %s", full_path)

      for chunk in chunker.chunk(full_path, file_stat.st_size, base_entry):
        digest = crypter.hash(chunk)
        if not index.find(digest):
          logger.debug("new chunk: %s", digest.encode("hex"))

          if not data_file:
            data_file_slot = index.reserve_file_slot()
            data_file = DataFile(
              config.tmp_dir_location(),
              config.max_data_file_size(),
              crypter)

          if not data_file.fits(chunk):
            # upload the file
            tree_hash = data_file.finalize()
            index.add_data_file_pre_upload(data_file_slot, tree_hash)

            archive_id, tree_hash = glacier_client.upload_file(
                data_file.file_name(), data_file.tree_hasher(), description={"backup": str(config.uuid()), "type": "data", "tree-hash": "%s" % tree_hash.encode("hex")})

            index.finalize_data_file(data_file_slot, tree_hash, archive_id)
            data_file.delete()
            data_file = None

          offset, length = data_file.add(chunk)
          index.add(digest, len(chunk), data_file_slot, offset, length)
        digests.append(digest)

      catalog.add_file(full_path, file_stat, digests)
      global_catalog.add_file(full_path, file_stat, digests)
    else:
      catalog.add(full_path, file_stat)
      global_catalog.add(full_path, file_stat)

  if data_file:
    tree_hash = data_file.finalize()
    index.add_data_file_pre_upload(data_file_slot, tree_hash)

    archive_id, tree_hash = glacier_client.upload_file(
      data_file.file_name(), data_file.tree_hasher(), description={"backup": str(config.uuid()), "type": "data", "tree-hash": "%s" % tree_hash.encode("hex")})

    index.finalize_data_file(data_file_slot, tree_hash, archive_id)
    data_file.delete()

  index.close()
  catalog.close()
  global_catalog.close()

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


def upload_catalog_command(args):
  assert not args.catalog.startswith("_")
  config = BackupConfiguration.LoadFromFile(args.config)

  assert os.path.isdir(config.catalog_dir_location()), "catalog directory does not exist: " + config.catalog_dir_location()
  assert os.path.isdir(config.tmp_dir_location()), "temp directory does not exist: " + config.tmp_dir_location()

  crypter = Crypter(config.secret_key())
  glacier_client = GlacierClient(
    config.aws_region(),
    config.aws_account_id(),
    config.vault_name(),
    config.aws_access_key(),
    config.aws_secret_access_key(),
    config.upload_chunk_size())

  index = Index(config.index_dir_location())
  catalog = Catalog(config.catalog_dir_location(), args.catalog, truncate=False)

  file_path = os.path.join(
    config.tmp_dir_location(),
    "%s-%s.ci" % (args.catalog, datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")))
  f = open(file_path, "w")

  iv, cipher = crypter.new_cipher()
  f.write(iv)

  compressor = bz2.BZ2Compressor()

  count = 0
  for k, v in catalog.raw_entries():
    f.write(cipher.encrypt(compressor.compress(struct.pack(">LL", len(k), len(v)))))
    f.write(cipher.encrypt(compressor.compress(k)))
    f.write(cipher.encrypt(compressor.compress(v)))
    count += 1

  logger.info("wrote %d catalog entries", count)

  count = 0
  for k, v in index.raw_entries():
    f.write(cipher.encrypt(compressor.compress(struct.pack(">LL", len(k), len(v)))))
    f.write(cipher.encrypt(compressor.compress(k)))
    f.write(cipher.encrypt(compressor.compress(v)))
    count += 1

  logger.info("wrote %d index entries", count)

  f.write(cipher.encrypt(compressor.flush()))
  f.close()

  index.close()
  catalog.close()

  glacier_client.upload_file(
    file_path, {"backup": str(config.uuid()), "type": "catalog/index", "catalog": args.catalog})

  os.unlink(file_path)

def search_catalog_command(args):
  patterns = [re.compile(x.decode("utf8")) for x in args.reg_exps]

  assert not args.catalog.startswith("_")
  config = BackupConfiguration.LoadFromFile(args.config)

  assert os.path.isdir(config.catalog_dir_location()), "catalog directory does not exist: " + config.catalog_dir_location()
  assert os.path.isdir(config.tmp_dir_location()), "temp directory does not exist: " + config.tmp_dir_location()

  index = Index(config.index_dir_location())
  catalog = Catalog(config.catalog_dir_location(), args.catalog, truncate=False)

  files_needed = {}

  for full_path, entry in catalog.match(patterns):
    if entry.is_directory():
      type = "dir"
      additional = ""
    elif entry.is_link():
      type = "link"
      additional = " -> " + entry.target()
    elif entry.is_regular_file():
      type = "file"
      additional = " %0.3f MB" % (entry._size / 1024.0 / 1024.0)
    else:
      type = "misc"
      additional = ""
    print "  %s [%s]%s" % (full_path, type, additional)

    if entry.is_regular_file():
      for digest in entry.digests():
        index_entry = index.find(digest)
        print "    %s (file %d, offset %d, size %d/%d [%0.2f%%])" % (
          digest.encode("hex"), index_entry.file_id, index_entry.offset,
          index_entry.original_length, index_entry.persisted_length,
          100.0 * index_entry.persisted_length / index_entry.original_length)

        if not index_entry.file_id in files_needed:
          files_needed[index_entry.file_id] = set()
        first_mb_block = int(round(index_entry.offset / 1024.0 / 1024.0))
        last_mb_block = int(round((index_entry.offset + index_entry.persisted_length) / 1024.0 / 1024.0))
        for i in range(first_mb_block, last_mb_block + 1):
          files_needed[index_entry.file_id].add(i)

  print
  print "files needed for restore:"
  for index in sorted(files_needed.keys()):
    print "  %d (~%d MB)" % (index, len(files_needed[index]))

def experimental_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  glacier_client = GlacierClient(
    config.aws_region(),
    config.aws_account_id(),
    config.vault_name(),
    config.aws_access_key(),
    config.aws_secret_access_key(),
    config.upload_chunk_size())

  connection = httplib.HTTPSConnection(glacier_client._host)
  # glacier_client._initiateInventoryRetrieval(connection)
  # glacier_client._initiateArchiveRetrieval(connection, "Jf0lifwWZez9tw0H6lGxMeQXiHCVyW8TNjCmTdx0D93SqloCsnDvl3tnJjD0DBa2aqsqtHzMqPacsLgX3jdUrdShtexU-G4S1B3EBUGH_cr8DgvFvAL85IRVCe3rn7KvsBnDqG83-Q")
  glacier_client._listJobs(connection)
  list = json.loads(glacier_client._getJobOutput(connection, "YhYnrWzRV4YXmnLE4Vzo0AdRoJQfyDvoRTFiZcnM2gIexQyT_tobHB3wRXhYjwcncbo0qG9DvJ4OJmwzmMQUAxCvCeUj"))
  for archive in list[u"ArchiveList"]:
    archive_id = archive[u"ArchiveId"]
    # glacier_client._deleteArchive(connection, archive_id)

  data = glacier_client._getJobOutput(connection,
    "_KRktzs7_zwgPQ99xQnELuH-dn5rMntWdp5Ovupf5s6XAUN0OsH-xbrXummqkTUDF9jCsv-EQ6z92S3V_eETv9IK4sCA")
  print data.encode("hex")



parser = argparse.ArgumentParser(description="Tool for backing up files to Amazon's Glacier Service.")
subparsers = parser.add_subparsers(title="Supported commands", description="Offers a variety of commands", help="such as these")

backup_parser = subparsers.add_parser("backup", help="start backing-up some directories")
backup_parser.add_argument(
  "-c", "--config", help="config file for backup set", required=True)
backup_parser.add_argument(
  "--catalog", help="catalog name to use", required=False, default="default")
backup_parser.add_argument(
  "-d", "--dir", nargs="+", help="a set of directories to be backed-up", required=True)
backup_parser.set_defaults(fn=backup_command)

new_parser = subparsers.add_parser("new", help="creates a new back-up configuration")
new_parser.add_argument(
  "-c", "--config", help="name of the configuration file to be created", required=True)
new_parser.set_defaults(fn=new_command)

repair_parser = subparsers.add_parser("repair", help="repairs a broken index")
repair_parser.add_argument(
  "-c", "--config", help="name of the configuration file to be created", required=True)
repair_parser.set_defaults(fn=repair_command)

upload_catalog_parser = subparsers.add_parser("upload_catalog", help="uploads a catalog/index")
upload_catalog_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
upload_catalog_parser.add_argument(
  "--catalog", help="catalog to upload", required=True, default="default")
upload_catalog_parser.set_defaults(fn=upload_catalog_command)

search_catalog_parser = subparsers.add_parser("search_catalog", help="looks for files in a catalog")
search_catalog_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
search_catalog_parser.add_argument(
  "--catalog", help="catalog to search", default="default")
search_catalog_parser.add_argument(
  "reg_exps", help="regular expressions to match against", nargs="+")
search_catalog_parser.set_defaults(fn=search_catalog_command)

experimental_parser = subparsers.add_parser("experimental", help="looks for files in a catalog")
experimental_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
experimental_parser.set_defaults(fn=experimental_command)

args = parser.parse_args()
args.fn(args)
#import cProfile
#cProfile.run('args.fn(args)', 'cprofile.out')
import os
import re
import time
from aws import GlacierClient
import crypto
import hex
from catalog import Catalog
from config import BackupConfiguration
from index import Index
import logging

logger = logging.getLogger(__name__)

def DownloadArchives(glacier_client, dir, backup_id, archives):
  finished_ids = set()

  for archive_id, tree_hash in archives.items():
    path = os.path.join(dir, "%s.remote" % hex.b2h(tree_hash))
    if os.path.isfile(path):
      finished_ids.add(archive_id)

  while finished_ids != archives.keys():
    connection = glacier_client.NewConnection()
    jobs = glacier_client._listJobs(connection)
    print("found jobs:", jobs)

    pending_ids = set()
    for job in jobs:
      if job.IsArchiveRetrieval() and not job.GetArchiveId() in finished_ids:
        if job.CompletedSuccessfully() and job.GetArchiveId() in archives:
          if job.GetTreeHash() == archives[job.GetArchiveId()]:
            # download file
            path = os.path.join(dir, "%s.remote" % hex.b2h(job.GetTreeHash()))
            with open(path, "wb") as f:
              f.write(glacier_client._getJobOutput(connection, job.Id()))
            finished_ids.add(job.GetArchiveId())
        elif job.IsPending() and job.GetArchiveId() in archives:
          if job.GetTreeHash() == archives[job.GetArchiveId()]:
            pending_ids.add(job.GetArchiveId())

    missing_ids = archives.keys() - finished_ids - pending_ids
    for missing_id in missing_ids:
      glacier_client._initiateArchiveRetrieval(connection, missing_id)

    print("needed: ", archives.keys())
    print("finished: ", finished_ids)
    print("pending: ", pending_ids)
    print("missing: ", missing_ids)

    if finished_ids != archives.keys():
      time.sleep(15 * 60)


def search_catalog_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  patterns = [re.compile(x) for x in args.reg_exps]
  assert not args.catalog.startswith("_")

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
      additional = " %0.3f MB" % (entry._size / 1024 / 1024)
    else:
      type = "misc"
      additional = ""
    print("  %s [%s]%s" % (full_path, type, additional))

    if entry.is_regular_file():
      for digest in entry.digests():
        index_entry = index.find(digest)
        print("    %s (file %d, offset %d, size %d/%d [%0.2f%%])" % (
          hex.b2h(digest), index_entry.file_id, index_entry.offset,
          index_entry.original_length, index_entry.persisted_length,
          100 * index_entry.persisted_length / index_entry.original_length))

        if not index_entry.file_id in files_needed:
          files_needed[index_entry.file_id] = set()
        first_mb_block = round(index_entry.offset / 1024 / 1024)
        last_mb_block = round((index_entry.offset + index_entry.persisted_length) / 1024 / 1024)
        for i in range(first_mb_block, last_mb_block + 1):
          files_needed[index_entry.file_id].add(i)

  print()
  print("files needed for restore:")
  archive_ids_needed = {}
  for i in sorted(files_needed.keys()):
    print("  %d (~%d MB)" % (i, len(files_needed[i])))
    archive_ids_needed[index.GetArchiveId(i)] = index.GetTreeHash(i)

  glacier_client = GlacierClient.FromConfig(config)
  DownloadArchives(
    glacier_client, config.tmp_dir_location(), config.uuid(), archive_ids_needed)

  crypter = crypto.Crypter(config.secret_key())
  for full_path, entry in catalog.match(patterns):
    output = open("/tmp/output.bin", "wb")
    if entry.is_regular_file():
      for digest in entry.digests():
        index_entry = index.find(digest)
        tree_hash = index.GetTreeHash(index_entry.file_id)
        path = os.path.join(
          config.tmp_dir_location(), "%s.remote" % hex.b2h(tree_hash))
        f = open(path, "rb")
        f.seek(index_entry.offset)
        iv = f.read(16)
        ciphertext = f.read(index_entry.persisted_length - 16)
        plaintext = crypter.decrypt(iv, ciphertext)
        real_digest = crypter.hash(plaintext)
        assert real_digest == digest
        output.write(plaintext)
    output.close()


#  connection = glacier_client.NewConnection()
#  for file_id in sorted(files_needed.keys()):
#    print("retrieving file %d" % file_id)
#    archive_name = index.GetArchiveId(file_id)
#    glacier_client._initiateArchiveRetrieval(
#      connection, archive_name)

#  "Action": "ArchiveRetrieval",
#  "ArchiveId": "oUUnpyEAY3NGuR_9GDZ1_ClrG8M4Zg05Si6nJJi18i15bUt9epoi9MvEvKyO2ckhTxOO9Yv20oVRHH0WJPeErHsPr9CrGK5ikHPuh9Lrn9SsvQ5a_pEtUC6XT5zz7RRMHnWEp9dD9A",
#  "ArchiveSizeInBytes": 572,
#  "Completed": true,
#  "CompletionDate": "2012-09-29T11:50:14.317Z",
#  "CreationDate": "2012-09-29T07:34:52.846Z",
#  "InventorySizeInBytes": null,
#  "JobDescription": "test job",
#  "JobId": "oyZaRb-bRBxctERwImtBy6ucw1c7DJNoqw5f0XyeGelqGRMunPzPqSdekChlaV3x8hg96dziHlOvaPQUvnU-Rf0gai7m",
#  "SHA256TreeHash": "d3f3aaf2c9fafe2f1e66736050a6d6c36972b40af156632f9e85fdfa59ad5a24",
#  "SNSTopic": null,
#  "StatusCode": "Succeeded",
#  "StatusMessage": "Succeeded",
#  "VaultARN": "arn:aws:glacier:us-west-2:508145281669:vaults/test"

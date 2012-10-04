import stat
from gletscher.aws import GlacierClient
from gletscher.catalog import Catalog
from gletscher.chunker import FileChunker
from gletscher.config import BackupConfiguration
from gletscher.crypto import Crypter
from gletscher.data import DataFile
from gletscher.index import Index
from gletscher.scanner import FileScanner
from gletscher import hex
import logging

logger = logging.getLogger(__name__)

def backup_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)
  assert not args.catalog.startswith("_")

  crypter = Crypter(config.secret_key())
  glacier_client = GlacierClient.FromConfig(config)
  chunker = FileChunker(config.max_chunk_size())

  index = Index(config.index_dir_location())
  global_catalog = Catalog(config.catalog_dir_location(), "_global")
  catalog = Catalog(config.catalog_dir_location(), args.catalog, truncate=True)

  data_file = None

  scanner = FileScanner(args.files)
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
          logger.debug("new chunk: %s", hex.b2h(digest))

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
                data_file.file_name(), data_file.tree_hasher(), description={"backup": str(config.uuid()), "type": "data", "tree-hash": hex.b2h(tree_hash)})

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
      data_file.file_name(), data_file.tree_hasher(), description={"backup": str(config.uuid()), "type": "data", "tree-hash": hex.b2h(tree_hash)})

    index.finalize_data_file(data_file_slot, tree_hash, archive_id)
    data_file.delete()

  index.close()
  catalog.close()
  global_catalog.close()
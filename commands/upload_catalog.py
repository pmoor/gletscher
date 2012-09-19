import bz2
from datetime import datetime
import os
import struct
from aws import GlacierClient
from catalog import Catalog
from config import BackupConfiguration
from crypto import Crypter
from index import Index
import logging

logger = logging.getLogger(__name__)

def upload_catalog_command(args):
  assert not args.catalog.startswith("_")
  config = BackupConfiguration.LoadFromFile(args.config)

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
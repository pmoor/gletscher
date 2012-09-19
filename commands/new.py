import os
import sys
from config import BackupConfiguration
from index import Index
import logging

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
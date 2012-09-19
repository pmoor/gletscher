import os
import re
from catalog import Catalog
from config import BackupConfiguration
from index import Index
import logging

logger = logging.getLogger(__name__)

def search_catalog_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  patterns = [re.compile(x.decode("utf8")) for x in args.reg_exps]
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
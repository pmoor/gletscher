import os

class FileChunker(object):

  def __init__(self, block_size):
    self._block_size = int(block_size)

  def chunk(self, full_path, size, base_catalog_entry=None):
    with open(full_path, "rb") as f:
      while True:
        to_read = min(self._block_size, size)
        chunk = f.read(to_read)
        if not chunk:
          break
        size -= len(chunk)
        yield chunk

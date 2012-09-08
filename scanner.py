import os
import stat

class FileScanner(object):

  def __init__(self, directories):
    self._directories = directories

  def __iter__(self):
    for directory in self._directories:
      for root, dirs, files in os.walk(directory.decode("utf8"), topdown=False):
        dir_stat = os.lstat(root)
        yield root, dir_stat

        for path in files:
          full_path = os.path.join(root, path)
          file_stat = os.lstat(full_path)
          yield full_path, file_stat

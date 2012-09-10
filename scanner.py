import os
import stat

class FileScanner(object):

  def __init__(self, files):
    self._files = files

  def __iter__(self):
    for file in self._files:
      file = os.path.abspath(file.decode("utf8"))
      file_stat = os.lstat(file)
      if stat.S_ISDIR(file_stat.st_mode):
        for root, dirs, files in os.walk(file, topdown=False):
          dir_stat = os.lstat(root)
          yield root, dir_stat

          for path in files:
            full_path = os.path.join(root, path)
            file_stat = os.lstat(full_path)
            yield full_path, file_stat
      else:
        yield file, file_stat

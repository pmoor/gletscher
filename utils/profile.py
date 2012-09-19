import math
import os

sizes = {}

for path, _, files in os.walk("/"):
  for f in files:
    try:
      s = os.stat(os.path.join(path, f))
      size = int(s.st_size)
      if size:
        l = int(math.log(size) / math.log(2))
      else:
        l = 0
      if not l in sizes:
        sizes[l] = 1
      else:
        sizes[l] += 1
    except:
      pass

for k in sorted(sizes.keys()):
  print "%2d %d" % (k, sizes[k])

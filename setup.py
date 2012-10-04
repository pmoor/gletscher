from distutils.core import setup

setup(
  name="Gletscher",
  version="0.1",
  packages=[
    "gletscher",
    "gletscher.test",
    "gletscher.commands",
    "gletscher.utils",
  ],
  scripts=[
    "bin/gletscher",
  ],
  license="LICENSE.txt",
  description="Fast Incremental Backups with Amazon's Glacier Service",
  long_description=open("README.txt").read(),
  author="Patrick Moor",
  author_email="patrick@moor.ws",
  url="http://code.google.com/p/gletscher/",
  install_requires=[
    "PyCrypto >= 2.6",
  ],
)
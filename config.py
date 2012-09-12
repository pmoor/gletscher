import os
from Crypto import Random
from Crypto.Protocol.KDF import PBKDF2
from Crypto import Hash
from Crypto.Cipher import AES
import getpass
import bz2
import sys
import json
import uuid
import ConfigParser
from crypto import Crypter

class BackupConfiguration(object):

  @staticmethod
  def LoadFromFile(config_dir):
    config = ConfigParser.RawConfigParser()
    config.read(os.path.join(config_dir, "backup.config"))
    return BackupConfiguration(config_dir, config)

  @staticmethod
  def NewEmptyConfiguration(config_dir):
    id = uuid.uuid4()
    secret_key = Random.get_random_bytes(32)

    crypter = Crypter(secret_key)
    signature = crypter.hash(id.bytes)

    min_config = "\n".join([
      "# gletscher configuration",
      "",
      "[id]",
      "uuid = %s" % str(id),
      "key = %s" % secret_key.encode("hex"),
      "signature = %s" % signature.encode("hex"),
      "",
      "[aws]",
      "region = %s" % BackupConfiguration.Prompt("AWS Region", verifier=lambda x: len(x.strip()) > 0),
      "account_id = %d" % int(BackupConfiguration.Prompt("AWS Account ID", verifier=lambda x: int(x) > 0)),
      "access_key = %s" % BackupConfiguration.Prompt("AWS Access Key", verifier=lambda x: len(x.strip()) > 0),
      "secret_access_key = %s" % BackupConfiguration.Prompt("AWS Secret Access Key", verifier=lambda x: len(x.strip()) > 0),
      "",
      "[glacier]",
      "vault_name = %s" % BackupConfiguration.Prompt("Glacier Vault Name", verifier=lambda x: len(x.strip()) > 0),
      ""
      "[dirs]",
      "index = index",
      "catalogs = catalogs",
      "tmp = tmp",
      "",
      "[scanning]",
      "max_chunk_size = %d" % (32 * 1024 * 1024),
      "max_data_file_size = %d" % (4 * 1024 * 1024 * 1024),
      "upload_chunk_size = %d" % (8 * 1024 * 1024),
      "",
    ])

    f = open(os.path.join(config_dir, "backup.config"), "w")
    f.write(min_config)
    f.close()
    return BackupConfiguration.LoadFromFile(config_dir)

  def __init__(self, config_dir, config):
    self._config_dir = config_dir
    self._config = config
    crypter = Crypter(self.secret_key())
    signature = crypter.hash(self.uuid().bytes)
    assert signature == self._config.get("id", "signature").decode("hex")

  def secret_key(self):
    key = self._config.get("id", "key").decode("hex")
    assert len(key) == 32
    return key

  def max_chunk_size(self):
    return self._config.getint("scanning", "max_chunk_size")

  def max_data_file_size(self):
    return self._config.getint("scanning", "max_data_file_size")

  def upload_chunk_size(self):
    return self._config.getint("scanning", "upload_chunk_size")

  def index_dir_location(self):
    return os.path.join(self._config_dir, self._config.get("dirs", "index"))

  def tmp_dir_location(self):
    return os.path.join(self._config_dir, self._config.get("dirs", "tmp"))

  def catalog_dir_location(self):
    return os.path.join(self._config_dir, self._config.get("dirs", "catalogs"))

  def uuid(self):
    return uuid.UUID(self._config.get("id", "uuid"))

  def aws_region(self):
    return self._config.get("aws", "region")

  def aws_account_id(self):
    return self._config.getint("aws", "account_id")

  def vault_name(self):
    return self._config.get("glacier", "vault_name")

  def aws_access_key(self):
    return self._config.get("aws", "access_key")

  def aws_secret_access_key(self):
    return self._config.get("aws", "secret_access_key")

  @staticmethod
  def Prompt(prompt, verifier):
    while True:
      input = raw_input("%s: " % prompt)
      try:
        if verifier(input.strip()):
          return input.strip()
      except:
        pass
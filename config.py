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

class BackupConfiguration(object):

  @staticmethod
  def LoadFromFile(config_dir, passphrase=None):
    data = json.load(open(os.path.join(config_dir, "backup.config"), "r"))

    if passphrase is None:
      passphrase = getpass.getpass("Please enter the passphrase: ").strip()

    salt = data["confidential"]["pbkdf2_salt"].decode("hex")
    assert len(salt) == 8

    iv = data["confidential"]["aes_iv"].decode("hex")
    assert len(iv) == 16

    storage_key = PBKDF2(passphrase, salt, 32, count=1000, prf=lambda p,s: Hash.HMAC.new(p, s, Hash.SHA256).digest())
    assert len(storage_key) == 32

    cipher = AES.new(storage_key, AES.MODE_CFB, iv)
    secret_config = json.loads(bz2.decompress(cipher.decrypt(data["confidential"]["ciphertext"].decode("hex"))))

    config = BackupConfiguration(config_dir, secret_config["secret_key"].decode("hex"), passphrase, uuid.UUID(data["uuid"]))
    config._max_chunk_size = int(data["max_chunk_size"])
    config._max_data_file_size = int(data["max_data_file_size"])
    config._index_dir_location = data["index_dir_location"]
    config._tmp_dir_location = data["tmp_dir_location"]
    config._catalog_dir_location = data["catalog_dir_location"]
    config._aws_secret_access_key = secret_config["aws_secret_access_key"]
    config._aws_account_id = data["aws_account_id"]
    config._aws_region = data["aws_region"]
    config._aws_access_key = data["aws_access_key"]
    config._vault_name = data["vault_name"]
    config._backup_name = data["backup_name"]
    config._upload_chunk_size = int(data["upload_chunk_size"])
    return config

  @staticmethod
  def NewEmptyConfiguration(config_dir):
    secret_key = Random.get_random_bytes(32)
    passphrase = BackupConfiguration._PromptForPassphrase()
    config = BackupConfiguration(config_dir, secret_key, passphrase, uuid.uuid4())
    config._max_chunk_size = 32 * 1024 * 1024 # 32 MB
    config._max_data_file_size = 2 * 1024 * 1024 * 1024 # 2 GB
    config._upload_chunk_size = 8 * 1024 * 1024 # 16 MB
    config._index_dir_location = "index"
    config._tmp_dir_location = "tmp"
    config._catalog_dir_location = "catalogs"

    config._backup_name = BackupConfiguration.Prompt("Backup Name", verifier=lambda x: len(x.strip()) > 0)
    config._aws_account_id = int(BackupConfiguration.Prompt("AWS Account ID", verifier=lambda x: int(x) > 0))
    config._aws_region = BackupConfiguration.Prompt("AWS Region", verifier=lambda x: len(x.strip()) > 0)
    config._aws_access_key = BackupConfiguration.Prompt("AWS Access Key", verifier=lambda x: len(x.strip()) > 0)
    config._aws_secret_access_key = BackupConfiguration.Prompt("AWS Secret Access Key", verifier=lambda x: len(x.strip()) > 0)
    config._vault_name = BackupConfiguration.Prompt("Glacier Vault Name", verifier=lambda x: len(x.strip()) > 0)

    config.write()
    return BackupConfiguration.LoadFromFile(config_dir, passphrase=passphrase)

  def __init__(self, config_dir, secret_key, passphrase, uuid):
    assert len(secret_key) == 32
    self._config_dir = config_dir
    self._secret_key = secret_key
    self._passphrase = passphrase
    self._uuid = uuid

  def secret_key(self):
    return self._secret_key

  def max_chunk_size(self):
    return self._max_chunk_size

  def max_data_file_size(self):
    return self._max_data_file_size

  def index_dir_location(self):
    return os.path.join(self._config_dir, self._index_dir_location)

  def tmp_dir_location(self):
    return os.path.join(self._config_dir, self._tmp_dir_location)

  def catalog_dir_location(self):
    return os.path.join(self._config_dir, self._catalog_dir_location)

  def uuid(self):
    return self._uuid

  def aws_region(self):
    return self._aws_region

  def aws_account_id(self):
    return self._aws_account_id

  def vault_name(self):
    return self._vault_name

  def aws_access_key(self):
    return self._aws_access_key

  def aws_secret_access_key(self):
    return self._aws_secret_access_key

  def upload_chunk_size(self):
    return self._upload_chunk_size

  def write(self):
    secret_config = {
      "secret_key": self._secret_key.encode("hex"),
      "aws_secret_access_key": self._aws_secret_access_key
    }

    salt = Random.get_random_bytes(8)
    assert len(salt) == 8
    iv = Random.get_random_bytes(16)
    assert len(iv) == 16

    storage_key = PBKDF2(self._passphrase, salt, 32, count=1000, prf=lambda p,s: Hash.HMAC.new(p, s, Hash.SHA256).digest())
    assert len(storage_key) == 32

    cipher = AES.new(storage_key, AES.MODE_CFB, iv)
    ciphertext = cipher.encrypt(bz2.compress(json.dumps(secret_config)))

    config = {
      "aws_account_id": self._aws_account_id,
      "aws_region": self._aws_region,
      "aws_access_key": self._aws_access_key,
      "vault_name": self._vault_name,
      "backup_name": self._backup_name,
      "uuid": str(self._uuid),
      "max_chunk_size": self._max_chunk_size,
      "max_data_file_size": self._max_data_file_size,
      "upload_chunk_size": self._upload_chunk_size,
      "index_dir_location": self._index_dir_location,
      "tmp_dir_location": self._tmp_dir_location,
      "catalog_dir_location": self._catalog_dir_location,
      "confidential": {
        "pbkdf2_salt": salt.encode("hex"),
        "aes_iv": iv.encode("hex"),
        "ciphertext": ciphertext.encode("hex")
      }
    }

    temp_file_name = os.path.join(self._config_dir, "backup.config.tmp")
    f = open(temp_file_name, "w")
    f.write(json.dumps(config, indent=2))
    f.close()
    os.rename(temp_file_name, os.path.join(self._config_dir, "backup.config"))

  @staticmethod
  def _PromptForPassphrase():
    while True:
      passphrase = getpass.getpass("Please enter a passphrase: ").strip()
      if not passphrase:
        response = raw_input(
          "No passphrase supplied. If you really want to use the empty string as a passphrase, type \"NO PASSPHRASE\": ").strip()
        if response == "NO PASSPHRASE":
          return ""
      else:
        passphrase_2 = getpass.getpass("Please repeat the passphrase: ").strip()
        if passphrase_2 == passphrase:
          return passphrase
        else:
          print >>sys.stderr, "Passphrases do not match, please try again."

  @staticmethod
  def Prompt(prompt, verifier):
    while True:
      input = raw_input("%s: " % prompt)
      try:
        if verifier(input.strip()):
          return input.strip()
      except:
        pass
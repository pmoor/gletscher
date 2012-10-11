# Copyright 2012 Patrick Moor <patrick@moor.ws>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from Crypto import Random
import uuid
import configparser
from gletscher.crypto import Crypter
from gletscher import hex

class BackupConfiguration(object):
    @staticmethod
    def _Prompt(prompt, verifier):
        while True:
            line = input("%s: " % prompt)
            try:
                if verifier(line.strip()):
                    return line.strip()
            except:
                pass

    @staticmethod
    def LoadFromFile(config_dir):
        config = configparser.RawConfigParser()
        config.read(os.path.join(config_dir, "backup.config"))
        backup_config = BackupConfiguration(config_dir, config)

        assert os.path.isdir(backup_config.catalog_dir_location()), \
            "catalog directory does not exist"
        assert os.path.isdir(backup_config.tmp_dir_location()), \
            "tmp directory does not exist"

        logging.basicConfig(
            level=logging.DEBUG,
            filename=backup_config.log_file_location(),
            datefmt="%Y-%m-%d %H:%M:%S",
            format="%(asctime)s %(levelname)s "
                   "%(name)s#%(funcName)s: %(message)s")
        return backup_config

    @staticmethod
    def NewEmptyConfiguration(config_dir, prompt_command=None):
        if not prompt_command:
            prompt_command = BackupConfiguration._Prompt

        id = uuid.uuid4()
        secret_key = Random.get_random_bytes(32)
        crypter = Crypter(secret_key)
        signature = crypter.hash(id.bytes)

        min_config = "\n".join([
            "# gletscher configuration",
            "",
            "[id]",
            "uuid = %s" % str(id),
            "key = %s" % hex.b2h(secret_key),
            "signature = %s" % hex.b2h(signature),
            "",
            "[aws]",
            "region = %s" % prompt_command(
                "AWS Region", verifier=lambda x: len(x) > 0),
            "account_id = %d" % int(prompt_command(
                "AWS Account ID", verifier=lambda x: int(x) > 0)),
            "access_key = %s" % prompt_command(
                "AWS Access Key", verifier=lambda x: len(x) > 0),
            "secret_access_key = %s" % prompt_command(
                "AWS Secret Access Key", verifier=lambda x: len(x) > 0),
            "",
            "[glacier]",
            "vault_name = %s" % prompt_command(
                "Glacier Vault Name", verifier=lambda x: len(x) > 0),
            "",
            "[dirs]",
            "index = index",
            "catalogs = catalogs",
            "tmp = tmp",
            "",
            "[scanning]",
            "max_chunk_size = %d" % (32 * 1024 * 1024),
            "max_data_file_size = %d" % (2 * 1024 * 1024 * 1024),
            "upload_chunk_size = %d" % (8 * 1024 * 1024),
            "",
            ])

        for dir in ("index", "catalogs", "tmp"):
            os.mkdir(os.path.join(config_dir, dir))

        with open(os.path.join(config_dir, "backup.config"), "w") as f:
            f.write(min_config)
        return BackupConfiguration.LoadFromFile(config_dir)

    def __init__(self, config_dir, config):
        self._config_dir = config_dir
        self._config = config
        crypter = Crypter(self.secret_key())
        signature = crypter.hash(self.uuid().bytes)
        assert signature == hex.h2b(self._config.get("id", "signature")), \
            "calculated signature does not match signature in config file"

    def secret_key(self):
        key = hex.h2b(self._config.get("id", "key"))
        assert len(key) == 32
        return key

    def max_chunk_size(self):
        return self._config.getint("scanning", "max_chunk_size")

    def max_data_file_size(self):
        return self._config.getint("scanning", "max_data_file_size")

    def upload_chunk_size(self):
        return self._config.getint("scanning", "upload_chunk_size")

    def config_dir_location(self):
        return self._config_dir

    def index_dir_location(self):
        return os.path.join(
            self._config_dir, self._config.get("dirs", "index"))

    def tmp_dir_location(self):
        return os.path.join(
            self._config_dir, self._config.get("dirs", "tmp"))

    def catalog_dir_location(self):
        return os.path.join(
            self._config_dir, self._config.get("dirs", "catalogs"))

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

    def log_file_location(self):
        return os.path.join(self._config_dir, "log.txt")
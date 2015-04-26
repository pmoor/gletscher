# Copyright 2013 Patrick Moor <patrick@moor.ws>
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

import dbm
import os
import sys
from gletscher.config import BackupConfiguration
from gletscher.index import Index
import logging

logger = logging.getLogger(__name__)

def register(subparsers):
    new_parser = subparsers.add_parser(
        "initialize", help="initializes a new back-up configuration")
    new_parser.set_defaults(fn=command)

def command(args):
    if os.path.isdir(args.config):
        print("the configuration directory already exists at %s" % args.config, file=sys.stderr)
        sys.exit(1)
    os.mkdir(args.config)
    assert os.path.isdir(args.config)

    config = BackupConfiguration.NewEmptyConfiguration(args.config)
    dbm.gnu.open(config.index_file_location(), "nf", 0o600)
    dbm.gnu.open(config.global_catalog_location(), "nf", 0o600)

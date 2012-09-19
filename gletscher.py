import argparse
from commands.backup import backup_command
from commands.experimental import experimental_command
from commands.new import new_command
from commands.repair import repair_command
from commands.search_catalog import search_catalog_command
from commands.upload_catalog import upload_catalog_command

parser = argparse.ArgumentParser(description="Tool for backing up files to Amazon's Glacier Service.")
subparsers = parser.add_subparsers(title="Supported commands", description="Offers a variety of commands", help="such as these")

backup_parser = subparsers.add_parser("backup", help="start backing-up some directories")
backup_parser.add_argument(
  "-c", "--config", help="config file for backup set", required=True)
backup_parser.add_argument(
  "--catalog", help="catalog name to use", required=False, default="default")
backup_parser.add_argument(
  "-d", "--dir", nargs="+", help="a set of directories to be backed-up", required=True)
backup_parser.set_defaults(fn=backup_command)

new_parser = subparsers.add_parser("new", help="creates a new back-up configuration")
new_parser.add_argument(
  "-c", "--config", help="name of the configuration file to be created", required=True)
new_parser.set_defaults(fn=new_command)

repair_parser = subparsers.add_parser("repair", help="repairs a broken index")
repair_parser.add_argument(
  "-c", "--config", help="name of the configuration file to be created", required=True)
repair_parser.set_defaults(fn=repair_command)

upload_catalog_parser = subparsers.add_parser("upload_catalog", help="uploads a catalog/index")
upload_catalog_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
upload_catalog_parser.add_argument(
  "--catalog", help="catalog to upload", required=True, default="default")
upload_catalog_parser.set_defaults(fn=upload_catalog_command)

search_catalog_parser = subparsers.add_parser("search_catalog", help="looks for files in a catalog")
search_catalog_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
search_catalog_parser.add_argument(
  "--catalog", help="catalog to search", default="default")
search_catalog_parser.add_argument(
  "reg_exps", help="regular expressions to match against", nargs="+")
search_catalog_parser.set_defaults(fn=search_catalog_command)

experimental_parser = subparsers.add_parser("experimental", help="looks for files in a catalog")
experimental_parser.add_argument(
  "-c", "--config", help="configuration directory to use", required=True)
experimental_parser.set_defaults(fn=experimental_command)

args = parser.parse_args()
args.fn(args)
#import cProfile
#cProfile.run('args.fn(args)', 'cprofile.out')
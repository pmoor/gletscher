import httplib
import json
import re
import datetime
from aws import GlacierClient
from config import BackupConfiguration
import logging

logger = logging.getLogger(__name__)

def experimental_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  glacier_client = GlacierClient.FromConfig(config)

  connection = httplib.HTTPSConnection(glacier_client._host)
  # glacier_client._initiateInventoryRetrieval(connection)
  # glacier_client._initiateArchiveRetrieval(connection, "Jf0lifwWZez9tw0H6lGxMeQXiHCVyW8TNjCmTdx0D93SqloCsnDvl3tnJjD0DBa2aqsqtHzMqPacsLgX3jdUrdShtexU-G4S1B3EBUGH_cr8DgvFvAL85IRVCe3rn7KvsBnDqG83-Q")

  jobs = glacier_client._listJobs(connection)
  print json.dumps(jobs, indent=2, sort_keys=True)

  for job in jobs:
    if job[u"Action"] == u"InventoryRetrieval":
      creation_date = datetime.datetime.strptime(job[u"CreationDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
      completion_date = None
      age = None
      if job[u"CompletionDate"]:
        completion_date = datetime.datetime.strptime(job[u"CompletionDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        age = datetime.datetime.utcnow() - completion_date
        duration = completion_date - creation_date
        print "took %d h %d min to complete" % (duration.seconds // 3600, (duration.seconds % 3600) // 60)
        print "completed %d hours %d minutes ago" % (age.seconds // 3600, (age.seconds % 3600) // 60)

      status_code = job[u"StatusCode"].encode("utf8")
      if status_code == "InProgress":
        print "wait for completion"

      print repr(status_code) # InProgress, Succeeded


  list = json.loads(glacier_client._getJobOutput(connection, "SX2lj4y4Cv0CsVpJJQVQswmuI8rER1xO3Ujcf0rwTO6U5q1lWJJvXGL8D0P2xV8rcfSxtJspsflVSF9JPXrvWRlLaYs9"))
  #for archive in list[u"ArchiveList"]:
  #  archive_id = archive[u"ArchiveId"]
    # glacier_client._deleteArchive(connection, archive_id)

  data = glacier_client._getJobOutput(connection,
    "SX2lj4y4Cv0CsVpJJQVQswmuI8rER1xO3Ujcf0rwTO6U5q1lWJJvXGL8D0P2xV8rcfSxtJspsflVSF9JPXrvWRlLaYs9")
  j = json.loads(data)

  print json.dumps(j, indent=2, sort_keys=True)
  inventory_date = datetime.datetime.strptime(j[u"InventoryDate"], "%Y-%m-%dT%H:%M:%SZ")
  age = datetime.datetime.utcnow() - inventory_date
  print "inventory is %d h %d min old" % (age.seconds // 3600, (age.seconds % 3600) // 60)


#{
#"Action": "InventoryRetrieval",
#"ArchiveId": null,
#"ArchiveSizeInBytes": null,
#"Completed": false,
#"CompletionDate": null,
#"CreationDate": "2012-09-20T05:25:40.230Z",
#"InventorySizeInBytes": null,
#"JobDescription": "test job",
#"JobId": "SX2lj4y4Cv0CsVpJJQVQswmuI8rER1xO3Ujcf0rwTO6U5q1lWJJvXGL8D0P2xV8rcfSxtJspsflVSF9JPXrvWRlLaYs9",
#"SHA256TreeHash": null,
#"SNSTopic": null,
#"StatusCode": "InProgress",
#"StatusMessage": null,
#"VaultARN": "arn:aws:glacier:us-west-2:508145281669:vaults/test"
#}

#{
#"Action": "InventoryRetrieval",
#"ArchiveId": null,
#"ArchiveSizeInBytes": null,
#"Completed": true,
#"CompletionDate": "2012-09-20T09:14:48.609Z",
#"CreationDate": "2012-09-20T05:25:40.230Z",
#"InventorySizeInBytes": 7848,
#"JobDescription": "test job",
#"JobId": "SX2lj4y4Cv0CsVpJJQVQswmuI8rER1xO3Ujcf0rwTO6U5q1lWJJvXGL8D0P2xV8rcfSxtJspsflVSF9JPXrvWRlLaYs9",
#"SHA256TreeHash": null,
#"SNSTopic": null,
#"StatusCode": "Succeeded",
#"StatusMessage": "Succeeded",
#"VaultARN": "arn:aws:glacier:us-west-2:508145281669:vaults/test"
#}
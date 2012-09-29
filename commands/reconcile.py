import http.client
import json
import datetime
import uuid
from aws import GlacierClient, GlacierJob
from config import BackupConfiguration
import logging
import time
from index import Index
import hex

logger = logging.getLogger(__name__)

def FindRecentCompletedArchiveRetrievalJob(glacier_client, max_result_age):
  recent_available_jobs = set()
  while not recent_available_jobs:
    connection = http.client.HTTPSConnection(glacier_client._host)
    jobs = glacier_client._listJobs(connection)

    recent_pending_jobs = set()
    for job in jobs:
      if job.IsInventoryRetrieval():
        if job.CompletedSuccessfully() and job.ResultAge() < max_result_age:
          recent_available_jobs.add(job)
        elif job.IsPending() and job.CreationAge() < max_result_age:
          recent_pending_jobs.add(job)

    if not recent_available_jobs and not recent_pending_jobs:
      logger.info("no recent job available or pending - starting a new one")
      glacier_client._initiateInventoryRetrieval(connection)
      time.sleep(300)
    elif not recent_available_jobs:
      logger.info("no recent job available - waiting for pending to complete")
      time.sleep(300)
  most_recent_job = min(recent_available_jobs, key=GlacierJob.ResultAge)
  return most_recent_job


def reconcile_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  glacier_client = GlacierClient.FromConfig(config)
  # glacier_client._initiateArchiveRetrieval(connection, "Jf0lifwWZez9tw0H6lGxMeQXiHCVyW8TNjCmTdx0D93SqloCsnDvl3tnJjD0DBa2aqsqtHzMqPacsLgX3jdUrdShtexU-G4S1B3EBUGH_cr8DgvFvAL85IRVCe3rn7KvsBnDqG83-Q")

  most_recent_job = FindRecentCompletedArchiveRetrievalJob(
      glacier_client, datetime.timedelta(seconds=30 * 3600))

  connection = glacier_client.NewConnection()
  list = json.loads(bytes.decode(glacier_client._getJobOutput(connection, most_recent_job.Id())))

  available_data = set()
  for archive in list["ArchiveList"]:
    description = json.loads(archive["ArchiveDescription"])
    id = uuid.UUID(description["backup"])
    if id == config.uuid():
      if description["type"] == "data":
        archive_id = archive["ArchiveId"]
        tree_hash = hex.h2b(description["tree-hash"])

        available_data.add((archive_id, tree_hash))

  index = Index(config.index_dir_location())
  file_entries = index.FindAllFileEntries()
  for id, file_entry in file_entries.items():
    available = False
    for archive_id, tree_hash in available_data:
      if file_entry.tree_hash == tree_hash and file_entry.archive_id == archive_id:
        print("data is available for %d" % id)
        available = True
    if not available:
      print("data is _missing_ for %d" % id)


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

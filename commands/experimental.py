import httplib
import json
from aws import GlacierClient
from config import BackupConfiguration
import logging

logger = logging.getLogger(__name__)

def experimental_command(args):
  config = BackupConfiguration.LoadFromFile(args.config)

  glacier_client = GlacierClient(
    config.aws_region(),
    config.aws_account_id(),
    config.vault_name(),
    config.aws_access_key(),
    config.aws_secret_access_key(),
    config.upload_chunk_size())

  connection = httplib.HTTPSConnection(glacier_client._host)
  # glacier_client._initiateInventoryRetrieval(connection)
  # glacier_client._initiateArchiveRetrieval(connection, "Jf0lifwWZez9tw0H6lGxMeQXiHCVyW8TNjCmTdx0D93SqloCsnDvl3tnJjD0DBa2aqsqtHzMqPacsLgX3jdUrdShtexU-G4S1B3EBUGH_cr8DgvFvAL85IRVCe3rn7KvsBnDqG83-Q")
  glacier_client._listJobs(connection)
  list = json.loads(glacier_client._getJobOutput(connection, "YhYnrWzRV4YXmnLE4Vzo0AdRoJQfyDvoRTFiZcnM2gIexQyT_tobHB3wRXhYjwcncbo0qG9DvJ4OJmwzmMQUAxCvCeUj"))
  for archive in list[u"ArchiveList"]:
    archive_id = archive[u"ArchiveId"]
    # glacier_client._deleteArchive(connection, archive_id)

  data = glacier_client._getJobOutput(connection,
    "_KRktzs7_zwgPQ99xQnELuH-dn5rMntWdp5Ovupf5s6XAUN0OsH-xbrXummqkTUDF9jCsv-EQ6z92S3V_eETv9IK4sCA")
  print data.encode("hex")
import http.client
from datetime import datetime
import hashlib
import hmac
import json
import os
import stat
import time
import hex
import crypto
import logging
import uuid
import re

logger = logging.getLogger(__name__)

class GlacierJob(object):

  def __init__(self, js):
    self._js = js

  def Id(self):
    return self._js["JobId"]

  def IsInventoryRetrieval(self):
    return self._js["Action"] == "InventoryRetrieval"

  def CreationDate(self):
    return datetime.strptime(self._js["CreationDate"], "%Y-%m-%dT%H:%M:%S.%fZ")

  def CompletedSuccessfully(self):
    return self._js["Completed"] and self._js["StatusCode"] == "Succeeded"

  def CompletionDate(self):
    return datetime.strptime(self._js["CompletionDate"], "%Y-%m-%dT%H:%M:%S.%fZ")

  def ResultAge(self):
    return datetime.utcnow() - self.CompletionDate()

  def CreationAge(self):
    return datetime.utcnow() - self.CreationDate()

  def IsPending(self):
    return not self._js["Completed"]

  def __str__(self):
    return "GlacierJob[%s] { created at %s }" % (self.Id(), self.CreationDate())


class GlacierClient(object):

  @staticmethod
  def FromConfig(config):
    return GlacierClient(
      config.aws_region(),
      config.aws_account_id(),
      config.vault_name(),
      config.aws_access_key(),
      config.aws_secret_access_key(),
      config.upload_chunk_size())

  def __init__(self, aws_region, aws_account_id, vault_name, aws_access_key, aws_secret_access_key, upload_chunk_size):
    self._aws_region = aws_region
    self._aws_account_id = aws_account_id
    self._vault_name = vault_name
    self._aws_access_key = aws_access_key
    self._aws_secret_access_key = aws_secret_access_key
    self._upload_chunk_size = upload_chunk_size
    self._host = "glacier.%s.amazonaws.com" % self._aws_region

  def NewConnection(self):
    return http.client.HTTPSConnection(self._host)

  def _log_headers(self, response):
    logger.debug("response: %d %s\n%s" % (
      response.status, response.reason,
      "\n".join("  %s: %s" % x for x in sorted(response.getheaders()))))

  def _sha256(self, content):
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()

  def _compute_all_headers(self, method, path, headers={}, query_string="",  payload=b""):
    headers = dict(headers)
    full_date = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    day = full_date[:8]
    headers["Date"] = full_date
    headers["Host"] = self._host
    headers["x-amz-glacier-version"] = "2012-06-01"

    sorted_header_key_list = sorted([x for x in headers.keys()], key=lambda x: x.lower())
    canonical_headers = "".join(
      "%s:%s\n" % (key.lower(), headers[key].strip()) for key in sorted_header_key_list)
    signed_headers = ";".join(x.lower() for x in sorted_header_key_list)

    canonical = "%s\n%s\n%s\n%s\n%s\n%s" % (
      method, path, query_string, canonical_headers, signed_headers, self._sha256(payload))

    string_to_sign = "%s\n%s\n%s/%s/%s/%s\n%s" % (
      "AWS4-HMAC-SHA256", full_date, day, self._aws_region, "glacier", "aws4_request", self._sha256(str.encode(canonical)))

    k_date = hmac.new(str.encode("AWS4" + self._aws_secret_access_key), str.encode(day), digestmod=hashlib.sha256).digest()
    k_region = hmac.new(k_date, str.encode(self._aws_region), digestmod=hashlib.sha256).digest()
    k_service = hmac.new(k_region, b"glacier", digestmod=hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", digestmod=hashlib.sha256).digest()

    signature = hmac.new(k_signing, str.encode(string_to_sign), digestmod=hashlib.sha256).hexdigest()

    headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request,SignedHeaders=%s,Signature=%s" % (
      self._aws_access_key,
      day,
      self._aws_region,
      "glacier",
      signed_headers,
      signature)
    return headers

  def _initiateMultipartUpload(self, connection, part_size, description):
    path = "/%d/vaults/%s/multipart-uploads" % (self._aws_account_id, self._vault_name)
    headers = {
      "x-amz-part-size": "%d" % part_size,
      "x-amz-archive-description": description,
    }
    headers = self._compute_all_headers("POST", path, headers=headers)
    connection.request("POST", path, headers=headers)
    response = connection.getresponse()

    body = response.read()
    assert response.status == http.client.CREATED, "%d: %s" % (response.status, body)
    self._log_headers(response)
    return response.getheader("x-amz-multipart-upload-id")

  def _uploadPart(self, connection, upload_id, payload, tree_hash, offset):
    path = "/%d/vaults/%s/multipart-uploads/%s" % (self._aws_account_id, self._vault_name, upload_id)
    headers = {
      "x-amz-sha256-tree-hash": tree_hash,
      "x-amz-content-sha256": self._sha256(payload),
      "Content-Range": "bytes %d-%d/*" % (offset, offset + len(payload) - 1),
      "Content-Length": "%d" % len(payload),
      "Content-Type": "application/octet-stream"
    }
    headers = self._compute_all_headers("PUT", path, headers=headers, payload=payload)
    connection.request("PUT", path, body=payload, headers=headers)
    response = connection.getresponse()

    body = response.read()
    assert response.status == http.client.NO_CONTENT, "%d: %s" % (response.status, body)
    assert response.getheader("x-amz-sha256-tree-hash") == tree_hash, body
    self._log_headers(response)

  def _completeUpload(self, connection, upload_id, tree_hash, total_size):
    path = "/%d/vaults/%s/multipart-uploads/%s" % (self._aws_account_id, self._vault_name, upload_id)
    headers = {
      "x-amz-sha256-tree-hash": tree_hash,
      "x-amz-archive-size": "%d" % total_size,
    }
    headers = self._compute_all_headers("POST", path, headers=headers)
    connection.request("POST", path, headers=headers)
    response = connection.getresponse()

    body = response.read()
    assert response.status == http.client.CREATED, "%d: %s" % (response.status, body)
    assert response.getheader("x-amz-sha256-tree-hash") == tree_hash, body
    self._log_headers(response)
    return response.getheader("x-amz-archive-id")

  def _listPendingUploads(self, connection):
    path = "/%d/vaults/%s/multipart-uploads" % (self._aws_account_id, self._vault_name)
    headers = {}
    headers = self._compute_all_headers("GET", path, headers=headers)
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()

    assert response.status == http.client.OK, "%d: %s" % (response.status, response.reason)
    self._log_headers(response)
    body = json.loads(bytes.decode(response.read()))
    return body["UploadsList"]

  def _listParts(self, connection, upload_id):
    parts = []
    marker = None

    while True:
      query_string = "limit=1000"
      if marker:
        query_string += "&marker=%s" % marker
      path = "/%d/vaults/%s/multipart-uploads/%s" % (self._aws_account_id, self._vault_name, upload_id)
      headers = self._compute_all_headers("GET", path, query_string=query_string)
      connection.request("GET", path + "?" + query_string, headers=headers)
      response = connection.getresponse()

      assert response.status == http.client.OK, "%d: %s\n---%s\n---" % (response.status, response.reason, response.read())
      self._log_headers(response)
      body = json.loads(bytes.decode(response.read()))
      parts += body["Parts"]
      part_size = int(body["PartSizeInBytes"])
      if body["Marker"]:
        marker = body["Marker"]
      else:
        return part_size, parts

  def _abortPendingUpload(self, connection, upload_id):
    path = "/%d/vaults/%s/multipart-uploads/%s" % (self._aws_account_id, self._vault_name, upload_id)
    headers = {}
    headers = self._compute_all_headers("DELETE", path, headers=headers)
    connection.request("DELETE", path, headers=headers)
    response = connection.getresponse()
    body = response.read()
    assert response.status == http.client.NO_CONTENT, "%d: %s" % (response.status, body)
    self._log_headers(response)

  def _initiateInventoryRetrieval(self, connection):
    path = "/%d/vaults/%s/jobs" % (self._aws_account_id, self._vault_name)
    payload = str.encode(json.dumps({"Type": "inventory-retrieval", "Description": "test job", "Format": "JSON"}))
    headers = self._compute_all_headers("POST", path, payload=payload)
    connection.request("POST", path, headers=headers, body=payload)
    response = connection.getresponse()
    self._log_headers(response)
    response.read()

  def _initiateArchiveRetrieval(self, connection, archive_id):
    path = "/%d/vaults/%s/jobs" % (self._aws_account_id, self._vault_name)
    payload = str.encode(json.dumps({"Type": "archive-retrieval", "Description": "test job", "ArchiveId": archive_id}))
    headers = self._compute_all_headers("POST", path, payload=payload)
    connection.request("POST", path, headers=headers, body=payload)
    response = connection.getresponse()
    self._log_headers(response)
    response.read()

  def _listJobs(self, connection):
    path = "/%d/vaults/%s/jobs" % (self._aws_account_id, self._vault_name)
    headers = self._compute_all_headers("GET", path)
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()
    self._log_headers(response)
    return [GlacierJob(js) for js in json.loads(bytes.decode(response.read()))["JobList"]]

  def _getJobOutput(self, connection, job_id, range=None):
    path = "/%d/vaults/%s/jobs/%s/output" % (self._aws_account_id, self._vault_name, job_id)
    headers = {}
    if range:
      headers["Range"] = "bytes %d-%d" % range
    headers = self._compute_all_headers("GET", path, headers=headers)
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()
    self._log_headers(response)
    return response.read()

  def _deleteArchive(self, connection, archive_id):
    path = "/%d/vaults/%s/archives/%s" % (self._aws_account_id, self._vault_name, archive_id)
    headers = self._compute_all_headers("DELETE", path)
    connection.request("DELETE", path, headers=headers)
    response = connection.getresponse()
    self._log_headers(response)
    assert response.status == http.client.NO_CONTENT, "%d: %s" % (response.status, response.reason)

  def upload_file(self, file, existing_tree_hasher=None, description=None, pending_upload=None):
    logger.info("starting upload of %s", file)
    assert description or pending_upload

    f_stat = os.stat(file)
    assert stat.S_ISREG(f_stat.st_mode), "must be a regular file: " + file
    with open(file, "rb") as f:

      connection = http.client.HTTPSConnection(self._host)
      if description:
        description = json.dumps(description)
        assert len(description) < 1024

        pending_upload = self._initiateMultipartUpload(connection, self._upload_chunk_size, description)
        available_parts = set()
        chunk_size = self._upload_chunk_size
      else:
        # TODO(patrick): handle more than 1000 parts (marker)
        chunk_size, parts = self._listParts(connection, pending_upload)
        available_parts = set()
        for part in parts:
          start, end = re.match("(\d+)-(\d+)", part["RangeInBytes"]).groups()
          # TODO(patrick): remove [-64:] once Amazon bug is fixed
          available_parts.add((int(start), int(end), hex.h2b(part["SHA256TreeHash"])))
        logger.debug("available parts: %s", "\n".join(["%d-%d:%s" % (a, b, hex.b2h(c)) for a, b, c in available_parts]))

      start_time = time.time()
      total_size = f_stat.st_size
      tree_hasher = crypto.TreeHasher()
      for start in range(0, total_size, chunk_size):
        end = min(start + chunk_size, total_size)
        data = f.read(end - start)
        tree_hasher.update(data)

        tree_hash = tree_hasher.get_tree_hash(start, end)
        if existing_tree_hasher:
          existing_tree_hash = existing_tree_hasher.get_tree_hash(start, end)
          assert tree_hash == existing_tree_hash, "computed tree hash does not match expected hash"

        logger.debug("uploading %s [%d,%d)", hex.b2h(tree_hash), start, end)
        if (start, end, tree_hash) in available_parts:
          # TODO(patrick): this should be end-1, according to the amazon documentation
          logger.debug("this part is already available - skipping upload")
          continue
        self._uploadPart(connection, pending_upload, data, hex.b2h(tree_hash), start)

        logger.debug("completed %0.2f MB out of %0.2f MB (%0.3f MB/s)",
            end / 1024 / 1024,
            total_size / 1024 / 1024 ,
            end / 1024 / 1024  / (time.time() - start_time))

      tree_hash = tree_hasher.get_tree_hash(0, total_size)
      archive_id = self._completeUpload(connection, pending_upload, hex.b2h(tree_hash), total_size)

    return archive_id, tree_hash

  def find_pending_upload(self, backup_uuid, tree_hash):
    connection = http.client.HTTPSConnection(self._host)
    for pending_upload in self._listPendingUploads(connection):
      description = json.loads(pending_upload["ArchiveDescription"])
      their_uuid = uuid.UUID(description["backup"])
      if their_uuid == backup_uuid:
        their_tree_hash = hex.h2b(description["tree-hash"])
        if their_tree_hash == tree_hash:
          return pending_upload["MultipartUploadId"]
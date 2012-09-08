import httplib
from datetime import datetime
import hashlib
import hmac
import json
import os
import stat
import time
import crypto
import logging

logger = logging.getLogger(__name__)

class GlacierClient(object):

  def __init__(self, aws_region, aws_account_id, vault_name, aws_access_key, aws_secret_access_key, upload_chunk_size):
    self._aws_region = aws_region.encode("utf8")
    self._aws_account_id = aws_account_id
    self._vault_name = vault_name.encode("utf8")
    self._aws_access_key = aws_access_key.encode("utf8")
    self._aws_secret_access_key = aws_secret_access_key.encode("utf8")
    self._upload_chunk_size = upload_chunk_size
    self._host = "glacier.%s.amazonaws.com" % self._aws_region

  def _sha256(self, content):
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()

  def _compute_all_headers(self, method, path, headers={}, query_string="",  payload=""):
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
      "AWS4-HMAC-SHA256", full_date, day, self._aws_region, "glacier", "aws4_request", self._sha256(canonical))

    k_secret = self._aws_secret_access_key
    k_date = hmac.new("AWS4" + k_secret, day, digestmod=hashlib.sha256).digest()
    k_region = hmac.new(k_date, self._aws_region, digestmod=hashlib.sha256).digest()
    k_service = hmac.new(k_region, "glacier", digestmod=hashlib.sha256).digest()
    k_signing = hmac.new(k_service, "aws4_request", digestmod=hashlib.sha256).digest()

    signature = hmac.new(k_signing, string_to_sign, digestmod=hashlib.sha256).hexdigest()

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
    assert response.status == httplib.CREATED, "%d: %s" % (response.status, body)
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
    assert response.status == httplib.NO_CONTENT, "%d: %s" % (response.status, body)
    assert response.getheader("x-amz-sha256-tree-hash") == tree_hash, body

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
    assert response.status == httplib.CREATED, "%d: %s" % (response.status, body)
    assert response.getheader("x-amz-sha256-tree-hash") == tree_hash, body
    return response.getheader("x-amz-archive-id")

  def _listPendingUploads(self, connection):
    path = "/%d/vaults/%s/multipart-uploads" % (self._aws_account_id, self._vault_name)
    headers = {}
    headers = self._compute_all_headers("GET", path, headers=headers)
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()

    assert response.status == httplib.OK, response.status

    body = json.load(response)
    return [upload[u"MultipartUploadId"] for upload in body[u"UploadsList"]]

  def _abortPendingUpload(self, connection, upload_id):
    path = "/%d/vaults/%s/multipart-uploads/%s" % (self._aws_account_id, self._vault_name, upload_id)
    headers = {}
    headers = self._compute_all_headers("DELETE", path, headers=headers)
    connection.request("DELETE", path, headers=headers)
    response = connection.getresponse()
    body = response.read()
    assert response.status == httplib.NO_CONTENT, "%d: %s" % (response.status, body)

  def _initiateInventoryRetrieval(self, connection):
    path = "/%d/vaults/%s/jobs" % (self._aws_account_id, self._vault_name)
    payload = json.dumps({"Type": "inventory-retrieval", "Description": "test job", "Format": "JSON"})
    headers = self._compute_all_headers("POST", path, payload=payload)
    connection.request("POST", path, headers=headers, body=payload)
    response = connection.getresponse()
    body = response.read()
    print response.status
    print response.getheaders()
    print body

  def upload_file(self, file, description, existing_tree_hasher=None):
    return "123", "1" * 32
    description = json.dumps(description)
    assert len(description) < 1024

    logger.info("starting upload of %s (%s)", file, description)

    connection = httplib.HTTPSConnection(self._host)

    upload_id = self._initiateMultipartUpload(connection, self._upload_chunk_size, description)

    f = open(file, "r")
    f_stat = os.stat(file)
    assert stat.S_ISREG(f_stat.st_mode), "must be a regular file: " + file

    start_time = time.time()
    total_size = f_stat.st_size
    tree_hasher = crypto.TreeHasher()
    for start in xrange(0, total_size, self._upload_chunk_size):
      end = min(start + self._upload_chunk_size, total_size)

      data = f.read(end - start)
      tree_hasher.update(data)

      tree_hash = tree_hasher.get_tree_hash(start, end)
      if existing_tree_hasher:
        existing_tree_hash = existing_tree_hasher.get_tree_hash(start, end)
        assert tree_hash == existing_tree_hash, "computed tree hash does not match expected hash"

      logger.debug("uploading %s [%d,%d)", tree_hash.encode("hex"), start, end)
      self._uploadPart(connection, upload_id, data, tree_hash.encode("hex"), start)

      logger.debug("completed %0.2f MB out of %0.2f MB (%0.3f MB/s)",
          end / 1024.0 / 1024.0,
          total_size / 1024.0 / 1024.0 ,
          end / 1024.0 / 1024.0  / (time.time() - start_time))

    tree_hash = tree_hasher.get_tree_hash(0, total_size)
    archive_id = self._completeUpload(connection, upload_id, tree_hash.encode("hex"), total_size)

    f.close()

    for upload_id in self._listPendingUploads(connection):
      logger.info("deleting pending upload: %s", upload_id)
      self._abortPendingUpload(connection, upload_id)

    return archive_id, tree_hash
#
#aws = GlacierClient()
#connection = httplib.HTTPSConnection(aws._host)
#aws._initiateInventoryRetrieval(connection)
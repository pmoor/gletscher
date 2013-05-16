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

import datetime

import http.server
import http.client
import json
import re
import socketserver
import hashlib

from gletscher.crypto import TreeHasher
from gletscher.hex import b2h

JOBS = {}
NEXT_JOB_ID = 1

PENDING_UPLOADS = {}
NEXT_UPLOAD_ID = 1

ARCHIVES = {}
NEXT_ARCHIVE_ID = 1

def InitiateMultipartUpload(rfile, headers, account_id, vault_name):
    global NEXT_UPLOAD_ID
    part_size = int(headers["x-amz-part-size"])
    description = headers["x-amz-archive-description"]

    upload_id = "upload-%04x" % NEXT_UPLOAD_ID
    NEXT_UPLOAD_ID += 1

    PENDING_UPLOADS[upload_id] = {
        "part_size": part_size,
        "description": description,
        "parts": {}
    }

    return (http.client.CREATED, {
        "x-amz-multipart-upload-id": upload_id}, None)

def MultipartUpload(rfile, headers, account_id, vault_name, upload_id):
    tree_hash = headers["x-amz-sha256-tree-hash"]
    content_hash = headers["x-amz-content-sha256"]
    r = tuple(int(x) for x in re.match(r"bytes (\d+)-(\d+)/*", headers["Content-Range"]).groups())
    length = int(headers["Content-Length"])

    data = rfile.read(length)
    digest = hashlib.sha256()
    digest.update(data)
    calculated_content_hash = digest.hexdigest()
    if calculated_content_hash != content_hash:
        return (http.client.BAD_REQUEST, {}, None)

    tree_hasher = TreeHasher()
    tree_hasher.update(data)
    calculated_tree_hash = b2h(tree_hasher.get_tree_hash())
    if calculated_tree_hash != tree_hash:
        return (http.client.BAD_REQUEST, {}, None)

    if r[1] - r[0] + 1 != length:
        return (http.client.BAD_REQUEST, {}, None)

    if not upload_id in PENDING_UPLOADS:
        return (http.client.NOT_FOUND, {}, None)

    PENDING_UPLOADS[upload_id]["parts"][r] = data

    return (http.client.NO_CONTENT, {"x-amz-sha256-tree-hash": tree_hash}, None)

def CompleteMultipartUpload(rfile, headers, account_id, vault_name, upload_id):
    global NEXT_ARCHIVE_ID
    tree_hash = headers["x-amz-sha256-tree-hash"]
    size = int(headers["x-amz-archive-size"])

    if not upload_id in PENDING_UPLOADS:
        return (http.client.NOT_FOUND, {}, None)

    parts = PENDING_UPLOADS[upload_id]["parts"]

    sorted_ranges = sorted(parts.keys())
    sorted_parts = tuple(parts[k] for k in sorted_ranges)

    tree_hasher = TreeHasher()
    for part in sorted_parts:
        tree_hasher.update(part)
    calculated_tree_hash = b2h(tree_hasher.get_tree_hash())
    if calculated_tree_hash != tree_hash:
        return (http.client.BAD_REQUEST, {}, None)

    if size != sum(len(part) for part in sorted_parts):
        return (http.client.BAD_REQUEST, {}, None)

    current = 0
    for r in sorted_ranges:
        if r[0] != current:
            return (http.client.BAD_REQUEST, {}, None)
        if r[1] != size - 1 and r[1] - r[0] + 1 != PENDING_UPLOADS[upload_id]["part_size"]:
            return (http.client.BAD_REQUEST, {}, None)
        current = range[1] + 1

    archive_id = "archive-%04x" % NEXT_ARCHIVE_ID
    NEXT_ARCHIVE_ID += 1

    ARCHIVES[archive_id] = {
        "description": PENDING_UPLOADS[upload_id]["description"],
        "data": b"".join(sorted_parts),
        "created": datetime.datetime.utcnow(),
        "tree_hash": tree_hash,
    }

    print(ARCHIVES.keys())
    del PENDING_UPLOADS[upload_id]

    return (http.client.CREATED, {
        "x-amz-sha256-tree-hash": tree_hash,
        "x-amz-archive-id": archive_id}, None)

def GetJobs(rfile, headers, account_id, vault_name):
    for job in JOBS.values():
        if not job["Completed"]:
            creation_date = datetime.datetime.strptime(
                job["CreationDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if datetime.datetime.utcnow() - creation_date > datetime.timedelta(seconds=3):
                job["Completed"] = True
                job["CompletionDate"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                job["StatusCode"] = "Succeeded"
                job["StatusMessage"] = "Succeeded"
                if job["Action"] == "ArchiveRetrieval":
                    archive = ARCHIVES[job["ArchiveId"]]
                    job["SHA256TreeHash"] = archive["tree_hash"]
                    job["ArchiveSHA256TreeHash"] = archive["tree_hash"]
                    job["RetrievalByteRange"] = "0-%d" % (len(archive["data"]) - 1)
                    job["ArchiveSizeInBytes"] = len(archive["data"])
                else:
                    job["InventorySizeInBytes"] = 42

    js = {"JobList": list(JOBS.values())}
    jobs = str.encode(json.dumps(js))
    return (http.client.OK, {}, jobs)

def CreateJob(rfile, headers, account_id, vault_name):
    global NEXT_JOB_ID
    length = int(headers["Content-Length"])

    body = json.loads(bytes.decode(rfile.read(length)))

    job = {
        "Action": None,
        "ArchiveId": None,
        "ArchiveSHA256TreeHash": None,
        "ArchiveSizeInBytes": None,
        "Completed": False,
        "CompletionDate": None,
        "CreationDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "InventorySizeInBytes": None,
        "JobDescription": body["Description"],
        "JobId": "job-%04x" % NEXT_JOB_ID,
        "RetrievalByteRange": None,
        "SHA256TreeHash": None,
        "SNSTopic": None,
        "StatusCode": "InProgress",
        "StatusMessage": None,
        "VaultARN": "arn:aws:glacier:localhost:%s:vaults/%s" % (account_id, vault_name),
    }

    if body["Type"] == "inventory-retrieval":
        job["Action"] = "InventoryRetrieval"
    elif body["Type"] == "archive-retrieval":
        job["Action"] = "ArchiveRetrieval"
        job["ArchiveId"] = body["ArchiveId"]

    NEXT_JOB_ID += 1
    JOBS[job["JobId"]] = job
    return (http.client.OK, {}, None)

def GetJobOutput(rfile, headers, account_id, vault_name, job_name):
    job = JOBS[job_name]

    if job["Action"] == "InventoryRetrieval":
        archive_list = []
        for archive_id, archive in ARCHIVES.items():
            archive_list.append({
                "ArchiveId": archive_id,
                "ArchiveDescription": archive["description"],
                "CreationDate": archive["created"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "Size": len(archive["data"]),
                "SHA256TreeHash": archive["tree_hash"],
            })

        inventory = {
            "VaultARN": "arn:aws:glacier:localhost:%s:vaults/%s" % (account_id, vault_name),
            "InventoryDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "ArchiveList": archive_list}
        data = str.encode(json.dumps(inventory))
        return (http.client.OK, {}, data)
    else:
        archive_id = job["ArchiveId"]
        archive = ARCHIVES[archive_id]
        return (http.client.OK, {}, archive["data"])

HANDLERS = [
    ("POST", re.compile(r"^/(\d+)/vaults/([^/]+)/multipart-uploads$"), InitiateMultipartUpload),
    ("PUT",  re.compile(r"^/(\d+)/vaults/([^/]+)/multipart-uploads/(.+)$"), MultipartUpload),
    ("POST", re.compile(r"^/(\d+)/vaults/([^/]+)/multipart-uploads/(.+)$"), CompleteMultipartUpload),
    ("GET",  re.compile(r"^/(\d+)/vaults/([^/]+)/jobs$"), GetJobs),
    ("POST", re.compile(r"^/(\d+)/vaults/([^/]+)/jobs$"), CreateJob),
    ("GET",  re.compile(r"^/(\d+)/vaults/([^/]+)/jobs/([^/]+)/output$"), GetJobOutput)
]

class RequestHandler(http.server.BaseHTTPRequestHandler):

    def _handle(self, method_called):
        for method, expression, handler in HANDLERS:
            if method == method_called:
                m = expression.match(self.path)
                if m:
                    response_code, response_headers, content =\
                        handler(self.rfile, self.headers, *m.groups())

                    self.send_response(response_code)
                    for header, value in response_headers.items():
                        self.send_header(header, value)
                    content_length = 0
                    if content:
                        content_length = len(content)
                    self.send_header("Content-Length", "%d" % content_length)
                    self.end_headers()
                    if content:
                        self.wfile.write(content)
                    return

        self.send_error(404)

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")

    def do_GET(self):
        self._handle("GET")

class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    pass

http.server.BaseHTTPRequestHandler.protocol_version = "HTTP/1.1"
httpd = ThreadedHTTPServer(('', 8080), RequestHandler)
httpd.serve_forever()

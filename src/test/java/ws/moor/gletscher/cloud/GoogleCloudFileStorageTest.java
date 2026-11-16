/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ws.moor.gletscher.cloud;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.util.LegacyHashing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class GoogleCloudFileStorageTest {

  private static final byte[] PAYLOAD = "hello".getBytes(StandardCharsets.UTF_8);
  private static final HashCode PAYLOAD_MD5 = LegacyHashing.md5().hashBytes(PAYLOAD);
  private static final String PAYLOAD_MD5_BASE64 =
      BaseEncoding.base64().encode(PAYLOAD_MD5.asBytes());

  @Test
  public void testSeparateStorageClassesForMetadataAndData() throws Exception {
    List<String> requestBodies = new ArrayList<>();
    Storage client = createMockStorage(requestBodies);
    GoogleCloudFileStorage storage =
        new GoogleCloudFileStorage(
            client,
            "my-bucket",
            "prefix/",
            MoreExecutors.newDirectExecutorService(),
            new CostTracker(),
            Optional.of(StorageClass.STANDARD),
            Optional.of(StorageClass.ARCHIVE));

    storage
        .store(
            "backups/1",
            PAYLOAD,
            PAYLOAD_MD5,
            ImmutableMap.of(),
            CloudFileStorage.StoreOptions.builder()
                .setObjectType(CloudFileStorage.ObjectType.METADATA)
                .build())
        .get();
    storage
        .store(
            "blocks/aa/bb/1",
            PAYLOAD,
            PAYLOAD_MD5,
            ImmutableMap.of(),
            CloudFileStorage.StoreOptions.builder()
                .setObjectType(CloudFileStorage.ObjectType.DATA)
                .build())
        .get();

    assertThat(requestBodies).hasSize(2);
    assertThat(requestBodies.get(0)).contains("\"storageClass\":\"STANDARD\"");
    assertThat(requestBodies.get(1)).contains("\"storageClass\":\"ARCHIVE\"");
  }

  @Test
  public void testOmittedStorageClassesUseBucketDefault() throws Exception {
    List<String> requestBodies = new ArrayList<>();
    Storage client = createMockStorage(requestBodies);
    GoogleCloudFileStorage storage =
        new GoogleCloudFileStorage(
            client,
            "my-bucket",
            "prefix/",
            MoreExecutors.newDirectExecutorService(),
            new CostTracker());

    storage
        .store(
            "backups/1",
            PAYLOAD,
            PAYLOAD_MD5,
            ImmutableMap.of(),
            CloudFileStorage.StoreOptions.builder()
                .setObjectType(CloudFileStorage.ObjectType.METADATA)
                .build())
        .get();

    assertThat(requestBodies).hasSize(1);
    assertThat(requestBodies.get(0)).doesNotContain("storageClass");
  }

  private static Storage createMockStorage(List<String> requestBodies) {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (getStreamingContent() != null) {
                  getStreamingContent().writeTo(baos);
                }
                requestBodies.add(baos.toString(StandardCharsets.UTF_8));
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(200);
                response.setContentType("application/json");
                response.setContent(String.format("{\"md5Hash\":\"%s\"}", PAYLOAD_MD5_BASE64));
                return response;
              }
            };
          }
        };
    return new Storage.Builder(transport, GsonFactory.getDefaultInstance(), null).build();
  }
}

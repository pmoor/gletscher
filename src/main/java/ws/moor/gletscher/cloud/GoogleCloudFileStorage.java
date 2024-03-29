/*
 * Copyright 2018 Patrick Moor <patrick@moor.ws>
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.FluentLogger;
import com.google.common.hash.HashCode;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import ws.moor.gletscher.util.LegacyHashing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GoogleCloudFileStorage implements CloudFileStorage {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final Storage client;
  private final String bucket;
  private final String filePrefix;
  private final ListeningExecutorService executor;
  private final CostTracker costTracker;

  public GoogleCloudFileStorage(
      Storage client,
      String bucket,
      String filePrefix,
      ListeningExecutorService executor,
      CostTracker costTracker) {
    Preconditions.checkArgument(filePrefix.endsWith("/"));
    this.client = client;
    this.bucket = bucket;
    this.filePrefix = filePrefix;
    this.executor = executor;
    this.costTracker = costTracker;
  }

  public static Storage buildStorageWithCredentials(Path credentialFilePath) {
    try {
      HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
      Credential credential =
          GoogleCredential.fromStream(
              Files.newInputStream(credentialFilePath), httpTransport, jsonFactory)
              .createScoped(
                  ImmutableSet.of("https://www.googleapis.com/auth/devstorage.read_write"));
      return new Storage.Builder(httpTransport, jsonFactory, credential)
          .setApplicationName("Gletscher/1.0")
          .build();
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ListenableFuture<?> store(
      String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    return executor.submit(
        retryingCallable(
            () -> {
              try {
                StorageObject sob =
                    new StorageObject()
                        .setName(filePrefix + name)
                        .setMd5Hash(BaseEncoding.base64().encode(md5.asBytes()))
                        .setMetadata(metadata);
                ByteArrayContent content =
                    new ByteArrayContent("application/octet-stream", data);

                costTracker.trackInsert(metadata, data.length);
                Storage.Objects.Insert method = client.objects().insert(bucket, sob, content);
                method.setDisableGZipContent(true);
                if (!options.allowOverwriting) {
                  method.setIfGenerationMatch(0L);
                }
                method.setFields("md5Hash");
                if (data.length < 10 << 20) {
                  method.getMediaHttpUploader().setDirectUploadEnabled(true);
                }
                StorageObject response = method.execute();
                logger.atFine().log("response received: %s", response);

                String expectedMd5 = BaseEncoding.base64().encode(md5.asBytes());
                if (!expectedMd5.equals(response.getMd5Hash())) {
                  throw new IllegalStateException(
                      "expected md5: " + expectedMd5 + ", actual: " + response.getMd5Hash());
                }
                return null;
              } catch (HttpResponseException e) {
                if (e.getStatusCode() == 412 && !options.allowOverwriting) {
                  // precondition failed -> object already exists
                  throw new FileAlreadyExistsException(name);
                }
                throw e;
              }
            }));
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    return new AbstractIterator<FileHeader>() {
      String nextPageToken = null;
      Iterator<FileHeader> currentBatch = null;
      boolean done = false;
      long remaining = limit;

      @Override
      protected FileHeader computeNext() {
        if (currentBatch != null && currentBatch.hasNext()) {
          return currentBatch.next();
        } else if (done) {
          return endOfData();
        }

        Objects result;
        try {
          result =
              retry(
                  () -> {
                    costTracker.trackList();
                    Storage.Objects.List list = client.objects().list(bucket);
                    list.setPrefix(filePrefix + prefix);
                    list.setMaxResults(Math.min(1000, remaining));
                    list.setFields("nextPageToken,items(name,size,md5Hash,metadata/*)");
                    if (nextPageToken != null) {
                      list.setPageToken(nextPageToken);
                    }
                    return list.execute();
                  });
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }

        Preconditions.checkNotNull(result);
        List<FileHeader> batch = new ArrayList<>();
        for (StorageObject object : result.getItems()) {
          batch.add(toFile(object));
        }
        remaining -= batch.size();
        currentBatch = batch.iterator();
        if (batch.size() < 1000 || remaining <= 0) {
          done = true;
          nextPageToken = null;
        } else {
          nextPageToken = Preconditions.checkNotNull(result.getNextPageToken());
        }
        return computeNext();
      }
    };
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    return executor.submit(
        retryingCallable(() -> {
          Storage.Objects.Get get = client.objects().get(bucket, filePrefix + name);
          get.setFields("");
          try {
            costTracker.trackHead();
            get.execute();
            return true;
          } catch (HttpResponseException e) {
            if (e.getStatusCode() == 404) {
              return false;
            }
            throw e;
          }
        }));
  }

  @Override
  public ListenableFuture<byte[]> get(String name) {
    return executor.submit(
        retryingCallable(
            () -> {
              Storage.Objects.Get get = client.objects().get(bucket, filePrefix + name);
              get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
              try {
                HttpResponse httpResponse = get.executeMedia();
                byte[] data = ByteStreams.toByteArray(httpResponse.getContent());
                costTracker.trackGet(data.length);
                String hash =
                    httpResponse.getHeaders().getFirstHeaderStringValue("x-goog-hash");

                Pattern pattern = Pattern.compile("md5=([A-Za-z0-9=/+]+)");
                Matcher matcher = pattern.matcher(hash);
                if (matcher.find()) {
                  HashCode md5 =
                      HashCode.fromBytes(BaseEncoding.base64().decode(matcher.group(1)));
                  HashCode actualMd5 = LegacyHashing.md5().hashBytes(data);
                  if (actualMd5.equals(md5)) {
                    return data;
                  }
                  throw new IllegalStateException("mismatched md5");
                } else {
                  throw new IllegalStateException("mismatched md5");
                }
              } catch (HttpResponseException e) {
                if (e.getStatusCode() == 404) {
                  return null;
                }
                throw e;
              }
            }));
  }

  @Override
  public void close() {
    // no-op
  }

  private FileHeader toFile(StorageObject object) {
    String name = object.getName();
    Preconditions.checkArgument(name.startsWith(filePrefix));
    name = name.substring(filePrefix.length());
    return new FileHeader(
        name,
        HashCode.fromBytes(BaseEncoding.base64().decode(object.getMd5Hash())),
        object.getSize().intValueExact(),
        object.getMetadata());
  }

  private static <R> Callable<R> retryingCallable(Callable<R> callable) {
    return () -> retry(callable);
  }

  private static <R> R retry(Callable<R> callable) throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Duration nextSleepDuration = Duration.ofMillis(100);
    final Duration maxSleepDuration = Duration.ofMinutes(5);
    final Duration maxAttemptDuration = Duration.ofHours(1);

    while (true) {
      try {
        return callable.call();
      } catch (IOException e) {
        if (stopwatch.elapsed().plus(nextSleepDuration).compareTo(maxAttemptDuration) > 0) {
          IllegalStateException ise = new IllegalStateException("too many retries");
          ise.addSuppressed(e);
          throw ise;
        }
      }

      Uninterruptibles.sleepUninterruptibly(nextSleepDuration.toMillis(), TimeUnit.MILLISECONDS);
      nextSleepDuration = nextSleepDuration.multipliedBy(2);
      if (nextSleepDuration.compareTo(maxSleepDuration) > 0) {
        nextSleepDuration = maxSleepDuration;
      }
    }
  }
}

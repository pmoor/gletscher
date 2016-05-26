/*
 * Copyright 2016 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class CatalogStore {

  private final CloudFileStorage storage;
  private final Clock clock;
  private final DateTimeFormatter dateTimeFormatter;

  public CatalogStore(CloudFileStorage storage, Clock clock) {
    this.storage = storage;
    this.clock = clock;
    dateTimeFormatter = DateTimeFormatter
        .ofPattern("yyyy/MM/dd/HH:mm:ss", Locale.US)
        .withZone(clock.getZone());
  }

  public void store(PersistedBlock rootDirectory) {
    Instant now = clock.instant();
    String fileName = createFileName(now);

    Gletscher.BackupRecord.Builder builder = Gletscher.BackupRecord.newBuilder();
    builder.setRootDirectory(rootDirectory.toProto());
    builder.setCreationTimeMillis(now.toEpochMilli());
    byte[] data = builder.build().toByteArray();
    Futures.getUnchecked(storage.store(fileName, data, Hashing.md5().hashBytes(data), ImmutableMap.of()));
  }

  private String createFileName(Instant now) {
    return String.format("backups/%s", dateTimeFormatter.format(now));
  }

  public Gletscher.BackupRecord findLatestBackup() {
    CloudFileStorage.FileHeader last = Iterators.getLast(storage.listFiles("backups/"), null);
    if (last == null) {
      return null;
    }
    try {
      return Gletscher.BackupRecord.parseFrom(Futures.getUnchecked(storage.get(last.name)));
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}

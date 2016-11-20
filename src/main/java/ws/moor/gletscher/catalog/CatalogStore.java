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
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;

import java.nio.file.FileSystem;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class CatalogStore {

  private final FileSystem fs;
  private final CloudFileStorage storage;
  private final DateTimeFormatter dateTimeFormatter;

  public CatalogStore(FileSystem fs, CloudFileStorage storage) {
    this.fs = fs;
    this.storage = storage;
    dateTimeFormatter = DateTimeFormatter
        .ofPattern("yyyy-MM-dd-HH-mm-ss", Locale.US)
        .withZone(ZoneId.of("UTC"));
  }

  public void store(Catalog catalog) {
    String fileName = createFileName(catalog.getStartTime());
    byte[] data = catalog.toProto().toByteArray();
    Futures.getUnchecked(storage.store(fileName, data, Hashing.md5().hashBytes(data), ImmutableMap.of()));
  }

  private String createFileName(Instant now) {
    long orderKey = (Long.MAX_VALUE - now.toEpochMilli());
    return String.format("backups/%016x-%s", orderKey, dateTimeFormatter.format(now));
  }

  public Catalog getLatestCatalog() {
    return Iterables.getOnlyElement(findLastCatalogs(1));
  }

  public List<Catalog> findLastCatalogs(int count) {
    List<Catalog> catalogs = new ArrayList<>(count);

    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", count);
    while (it.hasNext()) {
      CloudFileStorage.FileHeader header = it.next();
      try {
        Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(Futures.getUnchecked(storage.get(header.name)));
        catalogs.add(Catalog.fromProto(fs, proto));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return catalogs;
  }
}

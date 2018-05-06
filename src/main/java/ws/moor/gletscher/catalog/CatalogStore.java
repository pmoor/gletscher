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

package ws.moor.gletscher.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.InvalidProtocolBufferException;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.LegacyHashing;

import java.nio.file.FileSystem;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CatalogStore {

  private static final String VERSION_META_KEY = "version";

  private final FileSystem fs;
  private final CloudFileStorage storage;
  private final BlockStore blockStore;
  private final DateTimeFormatter dateTimeFormatter;

  public CatalogStore(FileSystem fs, CloudFileStorage storage, BlockStore blockStore) {
    this.fs = fs;
    this.storage = storage;
    this.blockStore = blockStore;
    dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss", Locale.US).withZone(ZoneId.of("UTC"));
  }

  public PersistedBlock store(Catalog catalog) {
    String fileName = createFileName(catalog.getStartTime());
    byte[] data = catalog.toProto().toByteArray();

    PersistedBlock pb = Futures.getUnchecked(blockStore.store(data, true));
    byte[] bytes = pb.toProto().toByteArray();
    Futures.getUnchecked(storage.store(
        fileName,
        bytes,
        LegacyHashing.md5().hashBytes(bytes),
        ImmutableMap.of(VERSION_META_KEY, "1"),
        new CloudFileStorage.StoreOptions(true)));
    return pb;
  }

  private String createFileName(Instant now) {
    long orderKey = (Long.MAX_VALUE - now.toEpochMilli());
    return String.format("backups/%016x-%s", orderKey, dateTimeFormatter.format(now));
  }

  public Optional<Catalog> getLatestCatalog() {
    List<Catalog> catalogs = findLastCatalogs(1);
    return Optional.ofNullable(Iterables.getOnlyElement(catalogs, null));
  }

  public List<Catalog> findLastCatalogs(int count) {
    List<Catalog> catalogs = new ArrayList<>(count);

    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", count);
    while (it.hasNext()) {
      CloudFileStorage.FileHeader header = it.next();
      try {
        byte[] catalogFileBytes = Futures.getUnchecked(storage.get(header.name));
        if (header.metadata.containsKey(VERSION_META_KEY)
            && Integer.parseInt(header.metadata.get(VERSION_META_KEY)) == 1) {
          Gletscher.PersistedBlock pbProto = Gletscher.PersistedBlock.parseFrom(catalogFileBytes);
          PersistedBlock pb = PersistedBlock.fromProto(pbProto);
          byte[] catalogBytes = Futures.getUnchecked(blockStore.retrieve(pb));
          Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(catalogBytes);
          catalogs.add(Catalog.fromProto(pb, fs, proto));
        } else {
          Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(catalogFileBytes);
          // Store this old-style catalog as a block as well, and use its address.
          PersistedBlock pb = Futures.getUnchecked(blockStore.store(catalogFileBytes, true));
          catalogs.add(Catalog.fromProto(pb, fs, proto));
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return catalogs;
  }
}

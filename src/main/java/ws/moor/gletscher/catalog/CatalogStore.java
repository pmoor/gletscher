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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

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

    PersistedBlock pb =
        Futures.getUnchecked(
            blockStore.store(data, true, CloudFileStorage.ObjectType.METADATA));
    byte[] bytes = pb.toProto().toByteArray();
    Futures.getUnchecked(storage.store(
        fileName,
        bytes,
        LegacyHashing.md5().hashBytes(bytes),
        ImmutableMap.of(VERSION_META_KEY, "1"),
        CloudFileStorage.StoreOptions.builder()
            .setCacheContentsOnUpload(true)
            .setObjectType(CloudFileStorage.ObjectType.METADATA)
            .build()));
    return pb;
  }

  private String createFileName(Instant now) {
    long orderKey = (Long.MAX_VALUE - now.toEpochMilli());
    return String.format("backups/%016x-%s", orderKey, dateTimeFormatter.format(now));
  }

  public Catalog load(PersistedBlock address) {
    try {
      byte[] bytes = Futures.getUnchecked(blockStore.retrieve(address));
      Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(bytes);
      return Catalog.fromProto(address, fs, proto);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A stored backup snapshot pointer together with its catalog block (for v1) and parsed catalog. */
  public record Snapshot(
      String pointerName, Optional<PersistedBlock> catalogBlock, Catalog catalog) {}

  public Optional<Catalog> getLatestCatalog() {
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", 1);
    if (!it.hasNext()) {
      return Optional.empty();
    }
    return Optional.of(loadSnapshot(it.next(), true).catalog());
  }

  /** Lists up to {@code limit} snapshot pointer headers from newest to oldest. */
  public List<CloudFileStorage.FileHeader> listSnapshotHeaders(int limit) {
    List<CloudFileStorage.FileHeader> headers = new ArrayList<>();
    Iterator<CloudFileStorage.FileHeader> it = storage.listFiles("backups/", limit);
    while (it.hasNext()) {
      headers.add(it.next());
    }
    return headers;
  }

  /** Loads and parses a single snapshot pointer without modifying remote storage. */
  public Snapshot loadSnapshot(CloudFileStorage.FileHeader header) {
    return loadSnapshot(header, false);
  }

  private static boolean isVersion1(CloudFileStorage.FileHeader header) {
    return header.metadata.containsKey(VERSION_META_KEY)
        && Integer.parseInt(header.metadata.get(VERSION_META_KEY)) == 1;
  }

  private Snapshot loadSnapshot(CloudFileStorage.FileHeader header, boolean persistLegacyCatalog) {
    try {
      byte[] catalogFileBytes = Futures.getUnchecked(storage.get(header.name));
      if (isVersion1(header)) {
        Gletscher.PersistedBlock pbProto = Gletscher.PersistedBlock.parseFrom(catalogFileBytes);
        PersistedBlock pb = PersistedBlock.fromProto(pbProto);
        return new Snapshot(header.name, Optional.of(pb), load(pb));
      } else {
        Gletscher.Catalog proto = Gletscher.Catalog.parseFrom(catalogFileBytes);
        PersistedBlock pb = null;
        if (persistLegacyCatalog) {
          // Store this old-style catalog as a block as well, and use its address.
          pb =
              Futures.getUnchecked(
                  blockStore.store(catalogFileBytes, true, CloudFileStorage.ObjectType.METADATA));
        }
        return new Snapshot(header.name, Optional.ofNullable(pb), Catalog.fromProto(pb, fs, proto));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}

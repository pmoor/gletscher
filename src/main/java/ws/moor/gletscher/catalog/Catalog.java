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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import javax.annotation.Nullable;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public class Catalog {

  private final @Nullable
  PersistedBlock address;
  private final Instant startTime;
  private final Instant endTime;
  private final Map<CatalogPath, PersistedBlock> roots;
  private final @Nullable
  PersistedBlock baseCatalog;

  private Catalog(@Nullable PersistedBlock address, Instant startTime, Instant endTime, Map<CatalogPath, PersistedBlock> roots, @Nullable PersistedBlock baseCatalog) {
    this.address = address;
    this.startTime = startTime;
    this.endTime = endTime;
    this.roots = roots;
    this.baseCatalog = baseCatalog;
  }

  public static Catalog fromNewBackup(
          Instant startTime, Instant endTime, Map<Path, PersistedBlock> roots, Catalog baseCatalog) {
    ImmutableMap.Builder<CatalogPath, PersistedBlock> catalogPathRootsBuilder = ImmutableMap.builder();
    for (Map.Entry<Path, PersistedBlock> entry : roots.entrySet()) {
      catalogPathRootsBuilder.put(CatalogPath.fromLocalPath(entry.getKey()), entry.getValue());
    }
    return new Catalog(null, startTime, endTime, catalogPathRootsBuilder.build(), baseCatalog != null ? baseCatalog.getAddress() : null);
  }

  static Catalog fromProto(PersistedBlock pb, FileSystem fs, Gletscher.Catalog catalog) {
    Instant startTime = Instant.ofEpochMilli(catalog.getStartTimeMillis());
    Instant endTime = Instant.ofEpochMilli(catalog.getEndTimeMillis());
    ImmutableMap.Builder<CatalogPath, PersistedBlock> rootBuilder = ImmutableMap.builder();
    for (Map.Entry<String, Gletscher.PersistedBlock> entry : catalog.getRootsMap().entrySet()) {
      rootBuilder.put(CatalogPath.fromRootName(entry.getKey()), PersistedBlock.fromProto(entry.getValue()));
    }
    PersistedBlock baseCatalog = catalog.hasBaseCatalog() ? PersistedBlock.fromProto(catalog.getBaseCatalog()) : null;
    return new Catalog(pb, startTime, endTime, rootBuilder.build(), baseCatalog);
  }

  Gletscher.Catalog toProto() {
    Gletscher.Catalog.Builder builder = Gletscher.Catalog.newBuilder();
    builder.setStartTimeMillis(startTime.toEpochMilli());
    builder.setEndTimeMillis(endTime.toEpochMilli());
    for (Map.Entry<CatalogPath, PersistedBlock> entry : roots.entrySet()) {
      builder.putRoots(entry.getKey().asRootName(), entry.getValue().toProto());
    }
    if (baseCatalog != null) {
      builder.setBaseCatalog(baseCatalog.toProto());
    }
    return builder.build();
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Map<CatalogPath, PersistedBlock> getRoots() {
    return roots;
  }

  public PersistedBlock getRootBlock(CatalogPath root) {
    return roots.get(root);
  }

  public PersistedBlock getAddress() {
    return Preconditions.checkNotNull(address, "catalog not persisted yet");
  }

  @Override
  public String toString() {
    return "Catalog<" + startTime + ">";
  }

  public PersistedBlock getOnlyRootBlock() {
    return Iterables.getOnlyElement(roots.values());
  }

  public Optional<PersistedBlock> getBaseCatalog() {
    return Optional.ofNullable(baseCatalog);
  }
}

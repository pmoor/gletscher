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
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

public class Catalog {

  private final Instant startTime;
  private final Instant endTime;
  private final Map<Path, PersistedBlock> roots;

  private Catalog(Instant startTime, Instant endTime, Map<Path, PersistedBlock> roots) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.roots = roots;
  }

  public static Catalog fromNewBackup(Instant startTime, Instant endTime, Map<Path, PersistedBlock> roots) {
    return new Catalog(startTime, endTime, ImmutableMap.copyOf(roots));
  }

  public static Catalog fromProto(FileSystem fs, Gletscher.Catalog catalog) {
    Instant startTime = Instant.ofEpochMilli(catalog.getStartTimeMillis());
    Instant endTime = Instant.ofEpochMilli(catalog.getEndTimeMillis());
    ImmutableMap.Builder<Path, PersistedBlock> rootBuilder = ImmutableMap.builder();
    for (Map.Entry<String, Gletscher.PersistedBlock> entry : catalog.getRootsMap().entrySet()) {
      rootBuilder.put(fs.getPath(entry.getKey()), PersistedBlock.fromProto(entry.getValue()));
    }
    return new Catalog(startTime, endTime, rootBuilder.build());
  }

  public Gletscher.Catalog toProto() {
    Gletscher.Catalog.Builder builder = Gletscher.Catalog.newBuilder();
    builder.setStartTimeMillis(startTime.toEpochMilli());
    builder.setEndTimeMillis(endTime.toEpochMilli());
    for (Map.Entry<Path, PersistedBlock> entry : roots.entrySet()) {
      builder.putRoots(entry.getKey().toString(), entry.getValue().toProto());
    }
    return builder.build();
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Map<Path, PersistedBlock> getRoots() {
    return roots;
  }

  public PersistedBlock getRootBlock(Path root) {
    return roots.get(root);
  }

  @Override
  public String toString() {
    return "Catalog<" + startTime + ">";
  }

  public PersistedBlock getOnlyRootBlock() {
    return Iterables.getOnlyElement(roots.values());
  }
}

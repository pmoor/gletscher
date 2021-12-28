/*
 * Copyright 2021 Patrick Moor <patrick@moor.ws>
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
import ws.moor.gletscher.blocks.BlockStore;

import javax.annotation.Nullable;
import java.util.Iterator;

import static java.util.Collections.emptyIterator;

public final class CatalogReaders {
  public static CatalogReader empty() {
    return new CatalogReader() {
      @Nullable
      @Override
      public CatalogDirectory findDirectory(CatalogPath path) {
        return null;
      }

      @Override
      public Iterator<CatalogFile> walk() {
        return emptyIterator();
      }
    };
  }

  public static CatalogReader fromBlockStore(BlockStore blockStore, Catalog catalog) {
    return new RealCatalogReader(blockStore, catalog);
  }

  public static CatalogReader mapping(ImmutableMap<String, String> catalogPathMapping, CatalogReader delegate) {
    ImmutableMap.Builder<CatalogPath, CatalogPath> pojoPathMapBuilder = ImmutableMap.builder();
    for (ImmutableMap.Entry<String, String> entry : catalogPathMapping.entrySet()) {
      pojoPathMapBuilder.put(CatalogPath.fromHumanReadableString(entry.getKey()), CatalogPath.fromHumanReadableString(entry.getValue()));
    }
    return new MappingCatalogReader(pojoPathMapBuilder.build(), delegate);
  }

  private CatalogReaders() {}
}

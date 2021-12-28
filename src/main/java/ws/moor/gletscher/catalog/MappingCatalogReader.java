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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

class MappingCatalogReader implements CatalogReader {
  private final ImmutableMap<CatalogPath, CatalogPath> mapping;
  private final CatalogReader delegate;

  MappingCatalogReader(ImmutableMap<CatalogPath, CatalogPath> mapping, CatalogReader delegate) {
    this.mapping = mapping;
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public CatalogDirectory findDirectory(CatalogPath path) {
    CatalogDirectory directory = delegate.findDirectory(path);
    if (directory != null) {
      return directory;
    }

    for (Map.Entry<CatalogPath, CatalogPath> mappingEntry : mapping.entrySet()) {
      CatalogPath mapped = path.replacePrefix(mappingEntry.getKey(), mappingEntry.getValue());
      if (!mapped.equals(path)) {
        directory = delegate.findDirectory(mapped);
        if (directory != null) {
          return directory;
        }
      }
    }
    return null;
  }

  @Override
  public Iterator<CatalogFile> walk() {
    return delegate.walk();
  }
}

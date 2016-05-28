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

import java.nio.file.Path;
import java.util.List;

public class CompositeCatalogReader implements CatalogReader {

  private final List<CatalogReader> readers;

  public CompositeCatalogReader(List<CatalogReader> readers) {
    this.readers = readers;
  }

  @Override
  public FileInformation findFile(Path path) {
    for (CatalogReader reader : readers) {
      FileInformation file = reader.findFile(path);
      if (file != null) {
        return file;
      }
    }
    return null;
  }
}
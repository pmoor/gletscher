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

import ws.moor.gletscher.blocks.PersistedBlock;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public interface CatalogReader {
  FileInformation findFile(Path path);

  final class FileInformation {

    public final Path path;
    public final Instant lastModifiedTime;
    public final List<PersistedBlock> blockList;

    FileInformation(Path path, Instant lastModifiedTime, List<PersistedBlock> blockList) {
      this.path = path;
      this.lastModifiedTime = lastModifiedTime;
      this.blockList = blockList;
    }

    public long getOriginalSize() {
      return blockList.stream().collect(Collectors.summingLong(PersistedBlock::getOriginalLength));
    }
  }
}

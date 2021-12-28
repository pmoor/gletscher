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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.proto.Gletscher;

import javax.annotation.Nullable;
import java.nio.file.InvalidPathException;
import java.time.Instant;
import java.util.Iterator;

public interface CatalogReader {
    @Nullable
    CatalogDirectory findDirectory(CatalogPath path);

    Iterator<CatalogFile> walk();

    class CatalogFile {
        public final CatalogPath path;
        public final Instant lastModifiedTime;
        public final ImmutableList<PersistedBlock> blockList;

        CatalogFile(CatalogPath filePath, Gletscher.FileEntry proto) {
            this.path = filePath;
            this.lastModifiedTime = Instant.ofEpochMilli(proto.getLastModifiedMillis());

            ImmutableList.Builder<PersistedBlock> builder = ImmutableList.builder();
            for (Gletscher.PersistedBlock persistedBlock : proto.getBlockList()) {
                builder.add(PersistedBlock.fromProto(persistedBlock));
            }
            this.blockList = builder.build();
        }

        public long getOriginalSize() {
            return blockList.stream().mapToLong(PersistedBlock::getOriginalLength).sum();
        }
    }

    class CatalogDirectory {
        private final PersistedBlock address;
        private final Gletscher.Directory proto;
        private final ImmutableMap<String, CatalogFile> fileEntries;

        CatalogDirectory(CatalogPath dirPath, PersistedBlock address, Gletscher.Directory proto) {
            this.address = address;
            this.proto = proto;

            ImmutableMap.Builder<String, CatalogReader.CatalogFile> filesBuilder = ImmutableMap.builder();
            for (Gletscher.DirectoryEntry entry : proto.getEntryList()) {
                if (entry.getTypeCase() == Gletscher.DirectoryEntry.TypeCase.FILE) {
                    Gletscher.FileEntry fileProto = entry.getFile();
                    String fileName = fileProto.getName();
                    try {
                        CatalogPath filePath = dirPath.makeChild(fileName);
                        filesBuilder.put(fileName, new CatalogFile(filePath, fileProto));
                    } catch (InvalidPathException ipe) {
                        System.err.printf("\n*** failed to resolve \"%s\" from \"%s\": %s\n", fileName, dirPath, ipe);
                    }
                }
            }
            fileEntries = filesBuilder.build();
        }

        @Nullable
        public CatalogFile findFileInformation(String fileName) {
            return fileEntries.get(fileName);
        }

        public PersistedBlock getAddress() {
            return address;
        }

        public Gletscher.Directory getProto() {
            return proto;
        }
    }
}

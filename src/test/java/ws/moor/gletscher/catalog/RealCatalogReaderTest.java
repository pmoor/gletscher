/*
 * Copyright 2026 Patrick Moor <patrick@moor.ws>
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
import com.google.common.hash.HashCode;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Signer;

import javax.crypto.spec.SecretKeySpec;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class RealCatalogReaderTest {

  private FileSystem fs;
  private InMemoryCloudFileStorage backingStorage;
  private Map<String, Integer> getCounts;
  private BlockStore blockStore;
  private CatalogStore catalogStore;

  @Before
  public void setUp() {
    fs = Jimfs.newFileSystem(unix());
    backingStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());
    getCounts = new HashMap<>();
    CloudFileStorage countingStorage =
        new CloudFileStorage() {
          @Override
          public ListenableFuture<?> store(
              String name,
              byte[] data,
              HashCode md5,
              Map<String, String> metadata,
              StoreOptions options) {
            return backingStorage.store(name, data, md5, metadata, options);
          }

          @Override
          public Iterator<FileHeader> listFiles(String prefix, int limit) {
            return backingStorage.listFiles(prefix, limit);
          }

          @Override
          public ListenableFuture<Boolean> exists(String name) {
            return backingStorage.exists(name);
          }

          @Override
          public ListenableFuture<byte[]> get(String name, GetOptions options) {
            getCounts.merge(name, 1, Integer::sum);
            return backingStorage.get(name, options);
          }

          @Override
          public void close() {
            backingStorage.close();
          }
        };

    Signer signer = new Signer(new SecretKeySpec(new byte[32], "HmacSHA256"));
    blockStore = new BlockStore(countingStorage, signer);
    catalogStore = new CatalogStore(fs, countingStorage, blockStore);
  }

  @Test
  public void testVisitorSkipSubtreeAvoidsFetchingSkippedDirectoryBlock() {
    PersistedBlock skippedLeafBlock = storeDirectory(List.of(fileEntry("secret.txt", 42)));
    PersistedBlock skippedDirBlock =
        storeDirectory(List.of(dirEntry("nested", skippedLeafBlock)));
    PersistedBlock keptDirBlock = storeDirectory(List.of(fileEntry("visible.txt", 7)));
    PersistedBlock homeBlock =
        storeDirectory(
            List.of(
                dirEntry("skip", skippedDirBlock),
                dirEntry("keep", keptDirBlock),
                fileEntry("top.txt", 1)));
    PersistedBlock rootBlock = storeDirectory(List.of(dirEntry("home", homeBlock)));

    Catalog catalog =
        Catalog.fromNewBackup(
            java.time.Instant.ofEpochSecond(1000),
            java.time.Instant.ofEpochSecond(1010),
            ImmutableMap.of(fs.getPath("/"), rootBlock),
            null);
    catalogStore.store(catalog);
    getCounts.clear();

    List<String> events = new ArrayList<>();
    CatalogReader reader = CatalogReaders.fromBlockStore(blockStore, catalog);
    reader.walk(
        new CatalogReader.Visitor() {
          @Override
          public CatalogReader.VisitResult preVisitDirectory(
              CatalogPath path, PersistedBlock block) {
            events.add("pre:" + path.getHumanReadableString() + ":" + block);
            if (block.equals(skippedDirBlock)) {
              return CatalogReader.VisitResult.SKIP_SUBTREE;
            }
            return CatalogReader.VisitResult.CONTINUE;
          }

          @Override
          public CatalogReader.VisitResult visitFile(CatalogReader.CatalogFile file) {
            events.add("file:" + file.path.getHumanReadableString());
            return CatalogReader.VisitResult.CONTINUE;
          }

          @Override
          public CatalogReader.VisitResult postVisitDirectory(
              CatalogPath path, PersistedBlock block) {
            events.add("post:" + path.getHumanReadableString());
            return CatalogReader.VisitResult.CONTINUE;
          }
        });

    assertThat(events)
        .containsExactly(
            "pre:/:" + rootBlock,
            "pre:/home:" + homeBlock,
            "pre:/home/skip:" + skippedDirBlock,
            "pre:/home/keep:" + keptDirBlock,
            "file:/home/keep/visible.txt",
            "post:/home/keep",
            "file:/home/top.txt",
            "post:/home",
            "post:/")
        .inOrder();

    assertThat(getCounts).doesNotContainKey(BlockStore.toFileName(skippedDirBlock));
    assertThat(getCounts).doesNotContainKey(BlockStore.toFileName(skippedLeafBlock));
    assertThat(getCounts).containsEntry(BlockStore.toFileName(keptDirBlock), 1);
  }

  @Test
  public void testVisitorTerminateStopsWalkImmediately() {
    PersistedBlock dirABlock =
        storeDirectory(List.of(fileEntry("first.txt", 1), fileEntry("second.txt", 2)));
    PersistedBlock dirBBlock = storeDirectory(List.of(fileEntry("third.txt", 3)));
    PersistedBlock rootBlock =
        storeDirectory(List.of(dirEntry("a", dirABlock), dirEntry("b", dirBBlock)));

    Catalog catalog =
        Catalog.fromNewBackup(
            java.time.Instant.ofEpochSecond(1000),
            java.time.Instant.ofEpochSecond(1010),
            ImmutableMap.of(fs.getPath("/"), rootBlock),
            null);

    List<String> visitedFiles = new ArrayList<>();
    CatalogReader reader = CatalogReaders.fromBlockStore(blockStore, catalog);
    reader.walk(
        new CatalogReader.Visitor() {
          @Override
          public CatalogReader.VisitResult visitFile(CatalogReader.CatalogFile file) {
            visitedFiles.add(file.path.getHumanReadableString());
            return CatalogReader.VisitResult.TERMINATE;
          }
        });

    assertThat(visitedFiles).containsExactly("/a/first.txt");
    assertThat(getCounts).doesNotContainKey(BlockStore.toFileName(dirBBlock));
  }

  @Test
  public void testIteratorWalkYieldsAllFiles() {
    PersistedBlock dirABlock =
        storeDirectory(List.of(fileEntry("first.txt", 1), fileEntry("second.txt", 2)));
    PersistedBlock dirBBlock = storeDirectory(List.of(fileEntry("third.txt", 3)));
    PersistedBlock rootBlock =
        storeDirectory(List.of(dirEntry("a", dirABlock), dirEntry("b", dirBBlock)));

    Catalog catalog =
        Catalog.fromNewBackup(
            java.time.Instant.ofEpochSecond(1000),
            java.time.Instant.ofEpochSecond(1010),
            ImmutableMap.of(fs.getPath("/"), rootBlock),
            null);

    CatalogReader reader = CatalogReaders.fromBlockStore(blockStore, catalog);
    List<String> paths = new ArrayList<>();
    reader.walk().forEachRemaining(file -> paths.add(file.path.getHumanReadableString()));
    assertThat(paths).containsExactly("/a/first.txt", "/a/second.txt", "/b/third.txt").inOrder();
  }

  private PersistedBlock storeDirectory(List<Gletscher.DirectoryEntry> entries) {
    Gletscher.Directory dir = Gletscher.Directory.newBuilder().addAllEntry(entries).build();
    return Futures.getUnchecked(
        blockStore.store(dir.toByteArray(), true, CloudFileStorage.ObjectType.METADATA));
  }

  private static Gletscher.DirectoryEntry fileEntry(String name, int size) {
    return Gletscher.DirectoryEntry.newBuilder()
        .setFile(
            Gletscher.FileEntry.newBuilder()
                .setName(name)
                .setLastModifiedMillis(1000L)
                .addBlock(
                    Gletscher.PersistedBlock.newBuilder()
                        .setSignature(
                            com.google.protobuf.ByteString.copyFrom(new byte[32]))
                        .setOriginalSize(size)))
        .build();
  }

  private static Gletscher.DirectoryEntry dirEntry(String name, PersistedBlock block) {
    return Gletscher.DirectoryEntry.newBuilder()
        .setDirectory(
            Gletscher.ChildDirectoryEntry.newBuilder()
                .setName(name)
                .setBlock(block.toProto()))
        .build();
  }
}

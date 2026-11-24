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

package ws.moor.gletscher.commands;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.BlockStore;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogPath;
import ws.moor.gletscher.catalog.CatalogStore;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.PretendMissingCloudFileStorage;
import ws.moor.gletscher.proto.Gletscher;
import ws.moor.gletscher.util.Signer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Command(
    name = "verify",
    description = "Verifies snapshots, parent links, and referenced data blocks in the cloud.")
class VerifyCommand extends AbstractCommand {
  VerifyCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
    addSnapshotsOption(
        options,
        "Verify the last N snapshots, or 'all' including parent links (default: all).");
    addMaxListingAgeOption(options);
    options.addOption(
        Option.builder()
            .longOpt("pretend-missing")
            .hasArg()
            .argName("FILE")
            .desc("File containing remote object names (or gs:// URIs) to pretend do not exist.")
            .get());
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "Command does not accept arguments.");
    }

    int snapshotLimit = parseSnapshotsOption(this, commandLine, Integer.MAX_VALUE);
    if (commandLine.hasOption("pretend-missing")) {
      Set<String> pretendMissing =
          loadPretendMissing(commandLine.getOptionValues("pretend-missing"));
      cloudFileStorage = new PretendMissingCloudFileStorage(cloudFileStorage, pretendMissing);
      blockStore = new BlockStore(cloudFileStorage, new Signer(config.getSigningKey()));
      catalogStore = new CatalogStore(context.getFileSystem(), cloudFileStorage, blockStore);
      context.getStdOut().printf("Pretending %d object(s) do not exist.%n", pretendMissing.size());
    }

    List<CloudFileStorage.FileHeader> headers = catalogStore.listSnapshotHeaders(snapshotLimit);
    if (headers.isEmpty()) {
      context.getStdErr().println("No existing backups found - nothing to verify.");
      return -1;
    }

    Set<PersistedBlock> allBlocks = blockStore.listAllBlocks();
    boolean followParentLinks = snapshotLimit == Integer.MAX_VALUE;
    Verifier verifier = new Verifier(allBlocks, followParentLinks);
    for (CloudFileStorage.FileHeader header : headers) {
      verifier.verifySnapshotHeader(header);
    }
    verifier.drain();

    if (verifier.errors == 0) {
      context.getStdOut().printf("Verified %d snapshot(s): Catalog is complete.%n", headers.size());
      return 0;
    } else {
      context.getStdOut().printf("Verification failed with %d error(s).%n", verifier.errors);
      return -1;
    }
  }

  private Set<String> loadPretendMissing(String[] paths)
      throws IOException, InvalidUsageException {
    Set<String> missing = new LinkedHashSet<>();
    for (String rawPath : paths) {
      Path path = context.getFileSystem().getPath(rawPath);
      if (!Files.exists(path)) {
        throw new InvalidUsageException(this, "File not found for --pretend-missing: %s", rawPath);
      }
      if (Files.isDirectory(path)) {
        throw new InvalidUsageException(
            this, "--pretend-missing path is a directory: %s", rawPath);
      }
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(context.readFile(path), StandardCharsets.UTF_8))) {
        parsePretendMissingLines(reader, rawPath, missing);
      }
    }
    return missing;
  }

  private void parsePretendMissingLines(
      BufferedReader reader, String sourceName, Set<String> missing)
      throws IOException, InvalidUsageException {
    String expectedGsPrefix = null;
    String line;
    int lineNumber = 0;
    while ((line = reader.readLine()) != null) {
      lineNumber++;
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      String objectName;
      if (trimmed.startsWith("gs://")) {
        if (expectedGsPrefix == null) {
          String bucket = config.getBucketName();
          if (Strings.isNullOrEmpty(bucket)) {
            throw new InvalidUsageException(
                this,
                "gcs.bucket_name is required in configuration to parse gs:// URIs in %s:%d.",
                sourceName,
                lineNumber);
          }
          expectedGsPrefix = "gs://" + bucket + "/" + Strings.nullToEmpty(config.getFilePrefix());
        }
        if (!trimmed.startsWith(expectedGsPrefix) || trimmed.length() == expectedGsPrefix.length()) {
          throw new InvalidUsageException(
              this,
              "Invalid --pretend-missing entry '%s' at %s:%d; expected prefix '%s'.",
              trimmed,
              sourceName,
              lineNumber,
              expectedGsPrefix);
        }
        objectName = trimmed.substring(expectedGsPrefix.length());
      } else {
        objectName = trimmed;
      }
      if (!objectName.startsWith("blocks/") && !objectName.startsWith("backups/")) {
        context
            .getStdErr()
            .printf(
                "Warning: --pretend-missing entry '%s' at %s:%d does not start with 'blocks/' or 'backups/'.%n",
                trimmed,
                sourceName,
                lineNumber);
      }
      missing.add(objectName);
    }
  }

  private record CatalogCheck(Catalog catalog, String source) {}

  private record DirCheck(PersistedBlock block, CatalogPath path, String source) {}

  private final class Verifier {
    private final Set<PersistedBlock> allBlocks;
    private final boolean followParentLinks;
    private final Set<PersistedBlock> visitedCatalogs = new HashSet<>();
    private final Set<PersistedBlock> visitedDirectories = new HashSet<>();
    private final Deque<CatalogCheck> catalogQueue = new ArrayDeque<>();
    private final Deque<DirCheck> dirQueue = new ArrayDeque<>();
    private int errors = 0;

    Verifier(Set<PersistedBlock> allBlocks, boolean followParentLinks) {
      this.allBlocks = allBlocks;
      this.followParentLinks = followParentLinks;
    }

    void verifySnapshotHeader(CloudFileStorage.FileHeader header) {
      try {
        CatalogStore.Snapshot snapshot = catalogStore.loadSnapshot(header);
        snapshot
            .catalogBlock()
            .ifPresent(
                pb -> {
                  visitedCatalogs.add(pb);
                  if (!allBlocks.contains(pb)) {
                    reportError("missing catalog block: %s for %s", pb, header.name);
                  }
                });
        catalogQueue.add(new CatalogCheck(snapshot.catalog(), header.name));
      } catch (Exception e) {
        reportError("broken snapshot %s: %s", header.name, describeError(e));
      }
    }

    void drain() {
      while (!catalogQueue.isEmpty()) {
        CatalogCheck check = catalogQueue.pop();
        for (Map.Entry<CatalogPath, PersistedBlock> root : check.catalog().getRoots().entrySet()) {
          dirQueue.add(new DirCheck(root.getValue(), root.getKey(), check.source()));
        }
        if (followParentLinks) {
          check
              .catalog()
              .getBaseCatalog()
              .ifPresent(baseBlock -> checkBaseCatalog(baseBlock, check.source()));
        }
      }

      while (!dirQueue.isEmpty()) {
        DirCheck dirCheck = dirQueue.pop();
        if (!visitedDirectories.add(dirCheck.block())) {
          continue;
        }
        verifyDirectory(dirCheck);
      }
    }

    private void checkBaseCatalog(PersistedBlock baseBlock, String source) {
      if (!allBlocks.contains(baseBlock)) {
        reportError("missing base_catalog block: %s referenced by %s", baseBlock, source);
        return;
      }
      if (!visitedCatalogs.add(baseBlock)) {
        return;
      }
      try {
        Catalog baseCatalog = catalogStore.load(baseBlock);
        catalogQueue.add(new CatalogCheck(baseCatalog, "base_catalog of " + source));
      } catch (Exception e) {
        reportError(
            "broken base_catalog block: %s referenced by %s: %s",
            baseBlock, source, describeError(e));
      }
    }

    private void verifyDirectory(DirCheck dirCheck) {
      if (!allBlocks.contains(dirCheck.block())) {
        reportError(
            "missing directory block: %s for %s in %s",
            dirCheck.block(), dirCheck.path().getHumanReadableString(), dirCheck.source());
        return;
      }

      Gletscher.Directory dir;
      try {
        byte[] bytes = Futures.getUnchecked(blockStore.retrieve(dirCheck.block()));
        dir = Gletscher.Directory.parseFrom(bytes);
      } catch (Exception e) {
        reportError(
            "broken directory block: %s for %s in %s: %s",
            dirCheck.block(),
            dirCheck.path().getHumanReadableString(),
            dirCheck.source(),
            describeError(e));
        return;
      }

      if (followParentLinks && dir.hasPreviousVersion()) {
        PersistedBlock previous = PersistedBlock.fromProto(dir.getPreviousVersion());
        if (!allBlocks.contains(previous)) {
          reportError(
              "missing previous_version directory block: %s for %s in %s",
              previous, dirCheck.path().getHumanReadableString(), dirCheck.source());
        } else {
          dirQueue.add(
              new DirCheck(
                  previous, dirCheck.path(), "previous_version of " + dirCheck.source()));
        }
      }

      for (Gletscher.DirectoryEntry entry : dir.getEntryList()) {
        switch (entry.getTypeCase()) {
          case FILE -> verifyFileEntry(dirCheck.path(), entry.getFile());
          case DIRECTORY -> enqueueChildDirectory(dirCheck, entry.getDirectory());
          case SYMLINK, TYPE_NOT_SET -> {}
        }
      }
    }

    private void verifyFileEntry(CatalogPath dirPath, Gletscher.FileEntry file) {
      CatalogPath filePath = resolveChild(dirPath, file.getName());
      String pathLabel =
          filePath != null
              ? filePath.getHumanReadableString()
              : dirPath.getHumanReadableString() + "/" + file.getName();
      for (Gletscher.PersistedBlock pb : file.getBlockList()) {
        PersistedBlock block = PersistedBlock.fromProto(pb);
        if (!allBlocks.contains(block)) {
          reportError("missing block: %s in %s", block, pathLabel);
        }
      }
    }

    private void enqueueChildDirectory(DirCheck parent, Gletscher.ChildDirectoryEntry child) {
      CatalogPath childPath = resolveChild(parent.path(), child.getName());
      if (childPath == null) {
        reportError(
            "invalid child directory path '%s' under %s in %s",
            child.getName(), parent.path().getHumanReadableString(), parent.source());
        return;
      }
      dirQueue.add(
          new DirCheck(PersistedBlock.fromProto(child.getBlock()), childPath, parent.source()));
    }

    private CatalogPath resolveChild(CatalogPath dirPath, String name) {
      try {
        return dirPath.makeChild(name);
      } catch (InvalidPathException e) {
        return null;
      }
    }

    private void reportError(String format, Object... args) {
      errors++;
      context.getStdOut().println(String.format(format, args));
    }

    private static String describeError(Throwable t) {
      Throwable root = t.getCause() != null ? t.getCause() : t;
      return root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
    }
  }
}

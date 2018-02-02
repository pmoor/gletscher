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

import com.google.common.collect.ImmutableList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.catalog.Catalog;
import ws.moor.gletscher.catalog.CatalogReader;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Command(name = "contains", description = "Checks whether a local file is part of a backup.")
class ContainsCommand extends AbstractCommand {
  ContainsCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected void addCommandLineOptions(Options options) {
    addConfigFileOption(options);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (args.isEmpty()) {
      throw new InvalidUsageException(this, "Must provide at least one file name.");
    }

    ImmutableList.Builder<Path> pathBuilder = ImmutableList.builder();
    for (String file : args) {
      Path path = context.getFileSystem().getPath(file);
      if (!Files.isRegularFile(path)) {
        context.getStdErr().printf("not a regular file: %s\n", file);
        return -1;
      }
      pathBuilder.add(path);
    }
    StreamSplitter splitter = config.getStreamSplitter();
    Map<Path, List<PersistedBlock>> blocksByPath = new LinkedHashMap<>();
    for (Path path : pathBuilder.build()) {
      List<PersistedBlock> blocks = new ArrayList<>();
      try (InputStream is = Files.newInputStream(path)) {
        Iterator<byte[]> it = splitter.split(is);
        while (it.hasNext()) {
          byte[] block = it.next();
          Signer signer = new Signer(config.getSigningKey());
          blocks.add(new PersistedBlock(signer.computeSignature(block), block.length));
        }
      }
      blocksByPath.put(path, blocks);
    }

    for (Catalog catalog : catalogStore.findLastCatalogs(16)) {
      CatalogReader catalogReader = new CatalogReader(blockStore, catalog);
      Iterator<CatalogReader.FileInformation> it = catalogReader.walk();
      while (it.hasNext()) {
        CatalogReader.FileInformation file = it.next();
        for (Map.Entry<Path, List<PersistedBlock>> entry : blocksByPath.entrySet()) {
          if (file.blockList.equals(entry.getValue())) {
            context.getStdOut().printf("match with %s in %s\n", file.path, catalog);
          }
        }
      }
    }

    return 0;
  }
}

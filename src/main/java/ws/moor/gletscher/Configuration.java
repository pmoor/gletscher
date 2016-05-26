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

package ws.moor.gletscher;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import ws.moor.gletscher.blocks.PersistedBlock;
import ws.moor.gletscher.blocks.Signature;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

class Configuration {

  private final FileSystem fs;
  private final Properties properties;

  public Configuration(Path propertiesPath) throws IOException {
    this.fs = propertiesPath.getFileSystem();

    properties = new Properties();
    try (Reader reader = Files.newBufferedReader(propertiesPath)){
      properties.load(reader);
    }
  }

  public SecretKeySpec getSigningKey() {
    return new SecretKeySpec(BaseEncoding.base16().lowerCase().decode(properties.getProperty("secret_key")), Signer.MAC_ALGO);
  }

  public SecretKeySpec getEncryptionKey() {
    return new SecretKeySpec(BaseEncoding.base16().lowerCase().decode(properties.getProperty("secret_key")), Cryptor.KEY_ALGO);
  }

  public String getBucketName() {
    return properties.getProperty("gcs_bucket_name");
  }

  public Path getCredentialFilePath() {
    return fs.getPath(properties.getProperty("gcs_credentials"));
  }

  public String getFilePrefix() {
    return properties.getProperty("gcs_object_prefix", "");
  }

  public Path getLocalCacheDir() {
    return fs.getPath(properties.getProperty("cache_dir"));
  }

  public Set<Path> getSkippedPaths() {
    return ImmutableSet.of(getLocalCacheDir());
  }

  private int getMaxSplitSize() {
    return Integer.parseInt(properties.getProperty("max_split_size", String.valueOf(32 << 20)));
  }

  public StreamSplitter getStreamSplitter() {
    switch (properties.getProperty("split_algorithm", "rolling")) {
      case "rolling":
        return StreamSplitter.rollingHashSplitter(getMaxSplitSize());
      case "fixed":
        return StreamSplitter.fixedSizeSplitter(getMaxSplitSize());
      default:
        throw new IllegalArgumentException("unknown splitter type");
    }
  }
}

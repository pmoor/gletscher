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

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import ws.moor.gletscher.util.Cryptor;
import ws.moor.gletscher.util.Signer;
import ws.moor.gletscher.util.StreamSplitter;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

class Configuration {

  private final FileSystem fs;
  private final Map<String, Object> yaml;

  static Configuration fromLines(FileSystem fs, String... lines) {
    Yaml yaml = new Yaml(new SafeConstructor());
    Map<String, Object> result = (Map<String, Object>) yaml.load(Joiner.on('\n').join(lines));
    return new Configuration(fs, result);
  }

  static Configuration fromFile(Path configFile) throws IOException {
    Yaml yaml = new Yaml(new SafeConstructor());
    Map<String, Object> result =
        (Map<String, Object>) yaml.load(Files.newBufferedReader(configFile, StandardCharsets.UTF_8));
    return new Configuration(configFile.getFileSystem(), result);
  }

  private Configuration(FileSystem fs, Map<String, Object> yaml) {
    Preconditions.checkArgument((int) yaml.get("version") == 1);

    this.fs = fs;
    this.yaml = yaml;
  }

  public SecretKeySpec getSigningKey() {
    return new SecretKeySpec((byte[]) yaml.get("secret_key"), Signer.MAC_ALGO);
  }

  public SecretKeySpec getEncryptionKey() {
    return new SecretKeySpec((byte[]) yaml.get("secret_key"), Cryptor.KEY_ALGO);
  }

  public String getBucketName() {
    return (String) findGcsNode().get("bucket_name");
  }

  public Path getCredentialFilePath() {
    return fs.getPath((String) findGcsNode().get("credentials"));
  }

  public String getFilePrefix() {
    return (String) findGcsNode().get("object_prefix");
  }

  public Path getLocalCacheDir() {
    return fs.getPath((String) yaml.get("cache_dir"));
  }

  public Set<Path> getSkippedPaths() {
    return ImmutableSet.of(getLocalCacheDir());
  }

  private int getMaxSplitSize() {
    return (int) yaml.get("max_split_size");
  }

  public StreamSplitter getStreamSplitter() {
    switch ((String) yaml.get("split_algorithm")) {
      case "rolling":
        return StreamSplitter.rollingHashSplitter(getMaxSplitSize());
      case "fixed":
        return StreamSplitter.fixedSizeSplitter(getMaxSplitSize());
      default:
        throw new IllegalArgumentException("unknown splitter type");
    }
  }

  public boolean disableCache() {
    return (boolean) yaml.get("disable_cache");
  }

  private Map<String, Object> findGcsNode() {
    return (Map<String, Object>) yaml.get("gcs");
  }
}

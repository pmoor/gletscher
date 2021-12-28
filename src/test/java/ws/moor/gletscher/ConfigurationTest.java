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

package ws.moor.gletscher;

import com.google.common.jimfs.Jimfs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.file.FileSystem;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.jimfs.Configuration.unix;
import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class ConfigurationTest {

  @Test
  public void testFromString() {
    FileSystem fs = Jimfs.newFileSystem(unix());
    Configuration config =
        Configuration.fromLines(
            fs,
            "version: 1",
            "secret_key: !!binary AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "gcs:",
            "  credentials: /credential/file.json",
            "  bucket_name: bucket-name",
            "  object_prefix: \" prefix/\"",
            "disable_cache: false",
            "cache_dir: /tmp/cache.dir",
            "max_split_size: 42",
            "split_algorithm: rolling",
            "",
            "include:",
            " - /Users/pmoor",
            " - /Volumes/External",
            "",
            "exclude:",
            " - \\.DS_Store$",
            " - \\.fseventsd$",
            "mappings:",
            "  a: b",
            "  b: c");

    assertThat(config.getEncryptionKey().getEncoded()).isEqualTo(new byte[32]);
    assertThat(config.getSigningKey().getEncoded()).isEqualTo(new byte[32]);
    assertThat((Object) config.getCredentialFilePath())
        .isEqualTo(fs.getPath("/credential/file.json"));
    assertThat(config.getBucketName()).isEqualTo("bucket-name");
    assertThat(config.getFilePrefix()).isEqualTo(" prefix/");
    assertThat(config.disableCache()).isFalse();
    assertThat((Object) config.getLocalCacheDir()).isEqualTo(fs.getPath("/tmp/cache.dir"));
    assertThat(config.getStreamSplitter().getMaxBlockSize()).isEqualTo(42);
    assertThat(config.getIncludes())
        .containsExactly(fs.getPath("/Users/pmoor"), fs.getPath("/Volumes/External"));
    assertThat(config.getExcludes().stream().map(Pattern::pattern).collect(Collectors.toList()))
        .containsExactly("\\.DS_Store$", "\\.fseventsd$", "^\\Q/tmp/cache.dir\\E$");
    assertThat(config.getCatalogPathMapping()).containsExactly("a", "b", "b", "c");
  }
}

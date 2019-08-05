/*
 * Copyright 2019 Patrick Moor <patrick@moor.ws>
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

package ws.moor.gletscher.commands.testing;

import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Set;

public interface FileReadFailureInjector {
  InputStream wrap(Path path, InputStream is) throws IOException;

  class NoFailureInjector implements FileReadFailureInjector {
    @Override
    public InputStream wrap(Path path, InputStream is) {
      return is;
    }
  }

  class FailSpecificNamesInjector implements FileReadFailureInjector {
    private final Set<String> names;

    public FailSpecificNamesInjector(String... names) {
      this.names = ImmutableSet.copyOf(names);
    }

    @Override
    public InputStream wrap(Path path, InputStream is) {
      if (!names.contains(path.getFileName().toString())) {
        return is;
      }
      return new InputStream() {
        @Override
        public int read() throws IOException {
          throw new IOException("Injected Failure");
        }
      };
    }
  }
}

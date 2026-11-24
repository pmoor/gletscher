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

package ws.moor.gletscher.cloud;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Read-only {@link CloudFileStorage} wrapper that pretends a configured set of object names do not
 * exist.
 */
public class PretendMissingCloudFileStorage implements CloudFileStorage {

  private final CloudFileStorage delegate;
  private final Set<String> missingNames;

  public PretendMissingCloudFileStorage(CloudFileStorage delegate, Set<String> missingNames) {
    this.delegate = Preconditions.checkNotNull(delegate);
    this.missingNames = ImmutableSet.copyOf(missingNames);
  }

  @Override
  public ListenableFuture<?> store(
      String name, byte[] data, HashCode md5, Map<String, String> metadata, StoreOptions options) {
    throw new UnsupportedOperationException(
        "Cannot store files through PretendMissingCloudFileStorage");
  }

  @Override
  public Iterator<FileHeader> listFiles(String prefix, int limit) {
    if (limit <= 0) {
      return Collections.emptyIterator();
    }
    int matchingMissing = countMissingStartingWith(prefix);
    if (matchingMissing == 0) {
      return delegate.listFiles(prefix, limit);
    }

    int delegateLimit = (int) Math.min((long) limit + matchingMissing, Integer.MAX_VALUE);
    Iterator<FileHeader> delegateIterator = delegate.listFiles(prefix, delegateLimit);
    return new AbstractIterator<>() {
      private int remaining = limit;

      @Override
      protected FileHeader computeNext() {
        while (remaining > 0 && delegateIterator.hasNext()) {
          FileHeader next = delegateIterator.next();
          if (!missingNames.contains(next.name)) {
            remaining--;
            return next;
          }
        }
        return endOfData();
      }
    };
  }

  @Override
  public ListenableFuture<Boolean> exists(String name) {
    if (missingNames.contains(name)) {
      return Futures.immediateFuture(false);
    }
    return delegate.exists(name);
  }

  @Override
  public ListenableFuture<byte[]> get(String name, GetOptions options) {
    if (missingNames.contains(name)) {
      return Futures.immediateFuture(null);
    }
    return delegate.get(name, options);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private int countMissingStartingWith(String prefix) {
    int count = 0;
    for (String name : missingNames) {
      if (name.startsWith(prefix)) {
        count++;
      }
    }
    return count;
  }
}

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

import java.util.Arrays;
import java.util.Locale;

/** Google Cloud Storage object storage classes. */
public enum StorageClass {
  STANDARD,
  NEARLINE,
  COLDLINE,
  ARCHIVE;

  public static StorageClass parse(String value) {
    Preconditions.checkArgument(value != null, "storage class must not be null");
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid storage class '%s'. Valid values: %s",
              value, Arrays.toString(values())),
          e);
    }
  }
}

/*
 * Copyright 2017 Patrick Moor <patrick@moor.ws>
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

import com.google.common.util.concurrent.ListeningExecutorService;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CostTracker;

import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.FileSystem;
import java.time.Clock;

public interface CommandContext {
  FileSystem getFileSystem();

  InputStream getStdIn();

  PrintStream getStdOut();

  PrintStream getStdErr();

  Clock getClock();

  ListeningExecutorService getExecutor();

  void exit(int status);

  CloudFileStorage connectToCloud(Configuration config, CostTracker costTracker);
}

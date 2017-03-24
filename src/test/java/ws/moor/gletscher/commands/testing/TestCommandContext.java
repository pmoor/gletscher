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

package ws.moor.gletscher.commands.testing;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import ws.moor.gletscher.Configuration;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CostTracker;
import ws.moor.gletscher.commands.CommandContext;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.time.Clock;

public class TestCommandContext implements CommandContext {
  public FileSystem fs;
  public CloudFileStorage cloudStorage;

  public ListeningExecutorService executorService = MoreExecutors.newDirectExecutorService();
  public ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
  public ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
  public Integer status = null;

  public TestCommandContext() {}

  public TestCommandContext(FileSystem fs, CloudFileStorage cloudStorage) {
    this.fs = fs;
    this.cloudStorage = cloudStorage;
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public InputStream getStdIn() {
    return System.in;
  }

  @Override
  public PrintStream getStdOut() {
    return new PrintStream(stdOut);
  }

  @Override
  public PrintStream getStdErr() {
    return new PrintStream(stdErr);
  }

  @Override
  public Clock getClock() {
    return Clock.systemDefaultZone();
  }

  @Override
  public ListeningExecutorService getExecutor() {
    return executorService;
  }

  @Override
  public void exit(int status) {
    Preconditions.checkState(this.status == null);
    this.status = status;
  }

  @Override
  public CloudFileStorage connectToCloud(Configuration config, CostTracker costTracker) {
    return cloudStorage;
  }

  public String stdOutString() {
    return new String(stdOut.toByteArray(), StandardCharsets.UTF_8);
  }

  public String stdErrString() {
    return new String(stdErr.toByteArray(), StandardCharsets.UTF_8);
  }
}

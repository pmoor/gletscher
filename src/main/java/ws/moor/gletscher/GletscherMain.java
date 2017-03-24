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

package ws.moor.gletscher;

import com.google.api.services.storage.Storage;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import ws.moor.gletscher.cloud.CloudFileStorage;
import ws.moor.gletscher.cloud.CostTracker;
import ws.moor.gletscher.cloud.GoogleCloudFileStorage;
import ws.moor.gletscher.commands.CommandContext;
import ws.moor.gletscher.commands.Commands;

import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.time.Clock;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GletscherMain {

  private final CommandContext context;

  public GletscherMain(CommandContext context) {
    this.context = context;
  }

  public static void main(String[] args) throws Exception {
    GletscherMain main = new GletscherMain(new RealCommandContext());
    main.run(args);
  }

  public void run(String... args) throws Exception {
    context.exit(Commands.run(context, args));
  }

  private static class RealCommandContext implements CommandContext {

    private final ListeningExecutorService executor;

    RealCommandContext() {
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("gletscher-pool-%d")
          .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
          .build();
      executor = MoreExecutors.listeningDecorator(
          new ThreadPoolExecutor(4, 32, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1024), threadFactory));
    }

    @Override
    public void exit(int status) {
      System.exit(status);
    }

    @Override
    public CloudFileStorage connectToCloud(Configuration config, CostTracker costTracker) {
      Storage storage = GoogleCloudFileStorage.buildStorageWithCredentials(config.getCredentialFilePath());
      return new GoogleCloudFileStorage(storage, config.getBucketName(), config.getFilePrefix(), executor, costTracker);
    }

    @Override
    public FileSystem getFileSystem() {
      return FileSystems.getDefault();
    }

    @Override
    public InputStream getStdIn() {
      return System.in;
    }

    @Override
    public PrintStream getStdOut() {
      return System.out;
    }

    @Override
    public PrintStream getStdErr() {
      return System.err;
    }

    @Override
    public Clock getClock() {
      return Clock.systemDefaultZone();
    }

    @Override
    public ListeningExecutorService getExecutor() {
      return executor;
    }
  }
}

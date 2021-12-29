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

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.cloud.testing.InMemoryCloudFileStorage;
import ws.moor.gletscher.commands.testing.TestCommandContext;
import ws.moor.gletscher.testing.FileSystemTestRule;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class SearchCommandTest {

  @ClassRule public static FileSystemTestRule fs = FileSystemTestRule.unix();
  private static InMemoryCloudFileStorage inMemoryStorage;

  @BeforeClass
  public static void setUp() throws Exception {
    inMemoryStorage = new InMemoryCloudFileStorage(MoreExecutors.newDirectExecutorService());

    fs.writeFile("/config.properties", """
        version: 1
        max_split_size: 65536
        disable_cache: true
        include:
          - /home
        """);

    fs.createDirectories("/home");
    fs.writeFile("/home/a.txt", "A");
    fs.writeFile("/home/b.pdf", "AA");
    fs.writeFile("/home/c.jpg", "AAA");

    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);

    main.run("backup", "-c", "/config.properties");
    assertThat(context.status).isEqualTo(0);
  }

  @Test
  public void testSearchWithNoArguments() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).containsMatch("/home/a.txt: .* \\(1 bytes\\)");
    assertThat(context.stdOutString()).containsMatch("/home/b.pdf: .* \\(2 bytes\\)");
    assertThat(context.stdOutString()).containsMatch("/home/c.jpg: .* \\(3 bytes\\)");
  }

  @Test
  public void testSearchWithRegexp() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties", "--", "(a|b)\\.(txt|pdf)", "-b\\.pdf");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("/home/a.txt");
    assertThat(context.stdOutString()).doesNotContain("/home/b.pdf");
    assertThat(context.stdOutString()).doesNotContain("/home/c.jpg");
  }

  @Test
  public void testSearchFullName() throws Exception {
    TestCommandContext context = new TestCommandContext(fs.getFileSystem(), inMemoryStorage);
    GletscherMain main = new GletscherMain(context);
    main.run("search", "-c", "/config.properties", "^/home/c.jpg$");

    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdOutString()).contains("/home/c.jpg");
    assertThat(context.stdOutString()).doesNotContain("/home/a.txt");
    assertThat(context.stdOutString()).doesNotContain("/home/b.pdf");
  }
}

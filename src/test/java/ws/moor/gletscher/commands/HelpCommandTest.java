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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ws.moor.gletscher.GletscherMain;
import ws.moor.gletscher.commands.testing.TestCommandContext;

import static com.google.common.truth.Truth.assertThat;
import static java.lang.System.lineSeparator;

@RunWith(JUnit4.class)
public class HelpCommandTest {

  private TestCommandContext context;
  private GletscherMain main;

  @Before
  public void setUp() {
    context = new TestCommandContext();
    main = new GletscherMain(context);
  }

  @Test
  public void testNoArgs() throws Exception {
    main.run();
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
    assertThat(context.stdErrString()).contains("Invalid Usage: No command specified.");
  }

  @Test
  public void testExplicitHelp() throws Exception {
    main.run("help");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString()).contains("Usage:" + lineSeparator() + "  gletscher [command]" + lineSeparator());
  }

  @Test
  public void testHelpWithInvalidCommand() throws Exception {
    main.run("help", "invalid_command");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
    assertThat(context.stdErrString()).contains("Invalid Usage: Unknown command: invalid_command");
  }

  @Test
  public void testHelpWithValidCommand() throws Exception {
    main.run("help", "backup");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString()).contains("Usage:" + lineSeparator() + "  gletscher backup" + lineSeparator());
  }
}

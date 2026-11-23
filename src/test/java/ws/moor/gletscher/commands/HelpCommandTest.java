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
    assertThat(context.stdOutString())
        .isEqualTo(
            "Usage:"
                + lineSeparator()
                + "  gletscher backup [options]"
                + lineSeparator()
                + lineSeparator()
                + "Backup local files remotely."
                + lineSeparator()
                + lineSeparator()
                + "Options:"
                + lineSeparator()
                + "  -c, --config <FILE>  Path to the configuration file. (required)"
                + lineSeparator());
  }

  @Test
  public void testHelpWithListObjectsOptions() throws Exception {
    main.run("help", "list_objects");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    String out = context.stdOutString();
    assertThat(out).contains("Usage:" + lineSeparator() + "  gletscher list_objects [options]");
    assertThat(out).contains(lineSeparator() + "Options:" + lineSeparator());
    assertThat(out).contains("  -c, --config <FILE>");
    assertThat(out).contains("      --min-size <size>");
    assertThat(out).contains("      --gs-uri");
    assertThat(out).contains("      --max-listing-age <duration>");
  }

  @Test
  public void testHelpWithCommandWithoutOptions() throws Exception {
    main.run("help", "version");
    assertThat(context.status).isEqualTo(0);
    assertThat(context.stdErrString()).isEmpty();
    assertThat(context.stdOutString())
        .isEqualTo(
            "Usage:"
                + lineSeparator()
                + "  gletscher version"
                + lineSeparator()
                + lineSeparator()
                + "Returns the current Gletscher version."
                + lineSeparator());
  }

  @Test
  public void testHelpForAllCommands() throws Exception {
    for (Command command : Commands.getCommandMeta()) {
      TestCommandContext cmdContext = new TestCommandContext();
      new GletscherMain(cmdContext).run("help", command.name());
      assertThat(cmdContext.status).isEqualTo(0);
      assertThat(cmdContext.stdErrString()).isEmpty();
      assertThat(cmdContext.stdOutString()).contains("Usage:");
      assertThat(cmdContext.stdOutString()).doesNotContain("null");
    }
  }

  @Test
  public void testMissingRequiredOptionPrintsOptions() throws Exception {
    main.run("list_objects");
    assertThat(context.status).isEqualTo(-1);
    assertThat(context.stdOutString()).isEmpty();
    String err = context.stdErrString();
    assertThat(err).contains("Invalid Usage:");
    assertThat(err).contains("Options:");
    assertThat(err).contains("  -c, --config <FILE>");
    assertThat(err).contains("      --max-listing-age <duration>");
  }
}

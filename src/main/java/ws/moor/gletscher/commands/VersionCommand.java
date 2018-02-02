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

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

@Command(name = "version", description = "Returns the current Gletscher version.")
class VersionCommand extends AbstractCommand {
  VersionCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (!args.isEmpty()) {
      throw new InvalidUsageException(this, "version takes no arguments");
    }

    String versionString = readVersionString();
    context.getStdOut().println("Gletscher Version " + versionString);
    return 0;
  }

  private String readVersionString() throws IOException {
    Properties properties = new Properties();
    properties.load(
        getClass()
            .getResourceAsStream("/META-INF/maven/ws.moor.gletscher/gletscher/pom.properties"));
    return (String) properties.get("version");
  }
}

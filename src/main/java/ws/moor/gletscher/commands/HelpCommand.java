/*
 * Copyright 2016 Patrick Moor <patrick@moor.ws>
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

import java.io.PrintStream;
import java.util.List;

@Command(name = "help", description = "Prints help about command usage.")
class HelpCommand extends AbstractCommand {
  HelpCommand(CommandContext context) {
    super(context);
  }

  @Override
  protected int runInternal(CommandLine commandLine, List<String> args) throws Exception {
    if (args.isEmpty()) {
      printHelp(context.getStdOut());
    } else if (args.size() == 1) {
      Class<? extends AbstractCommand> commandClass = Commands.forName(args.get(0));
      if (commandClass == null) {
        throw new InvalidUsageException(this, "Invalid command: " + args.get(0));
      }
      Commands.newInstance(commandClass, context).printUsage(context.getStdOut());
    } else {
      throw new InvalidUsageException(this, "Help takes at most one argument");
    }
    return 0;
  }

  static void printHelp(PrintStream out) {
    out.println("Usage:");
    out.println("  gletscher [command]");
    out.println();
    out.println("Supported Commands:");

    List<Command> descriptions = Commands.getCommandMeta();
    int longestCommandName = 0;
    for (Command command : descriptions) {
      longestCommandName = Math.max(longestCommandName, command.name().length());
    }
    for (Command command : descriptions) {
      out.printf("  %" + longestCommandName + "s  %s\n", command.name(), command.description());
    }
  }
}

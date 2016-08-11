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

class InvalidUsageException extends Exception {
  private final AbstractCommand command;
  private final String format;
  private final Object[] args;

  InvalidUsageException(AbstractCommand command, String format, Object... args) {
    this.command = command;
    this.format = format;
    this.args = args;
  }

  InvalidUsageException(String format, Object... args) {
    this.command = null;
    this.format = format;
    this.args = args;
  }

  void emit(CommandContext context) {
    context.getStdErr().printf("Invalid Usage: " + format + "\n\n", args);
    if (command != null) {
      command.printUsage(context.getStdErr());
    } else {
      HelpCommand.printHelp(context.getStdErr());
    }
  }
}

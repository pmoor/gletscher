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

class InvalidUsageException extends Exception {
  private static final long serialVersionUID = 8946953028986605714L;

  private final transient AbstractCommand command;

  InvalidUsageException(AbstractCommand command, String format, Object... args) {
    super(formatMessage(format, args));
    this.command = command;
  }

  InvalidUsageException(String format, Object... args) {
    super(formatMessage(format, args));
    this.command = null;
  }

  private static String formatMessage(String format, Object... args) {
    return args.length == 0 ? format : String.format(format, args);
  }

  void emit(CommandContext context) {
    context.getStdErr().printf("Invalid Usage: %s\n\n", getMessage());
    if (command != null) {
      command.printUsage(context.getStdErr());
    } else {
      HelpCommand.printHelp(context.getStdErr());
    }
  }
}

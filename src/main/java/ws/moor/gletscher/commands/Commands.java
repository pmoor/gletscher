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

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Commands {

  private static final List<Class<? extends AbstractCommand>> ALL_COMMANDS =
      ImmutableList.<Class<? extends AbstractCommand>>builder()
          .add(BackupCommand.class)
          .add(ContainsCommand.class)
          .add(HelpCommand.class)
          .add(RestoreCommand.class)
          .add(SearchCommand.class)
          .add(StatsCommand.class)
          .add(VersionCommand.class)
          .build();

  static Class<? extends AbstractCommand> forName(String name) throws InvalidUsageException {
    for (Class<? extends AbstractCommand> command : ALL_COMMANDS) {
      Command annotation = command.getAnnotation(Command.class);
      if (annotation.name().equals(name)) {
        return command;
      }
    }
    throw new InvalidUsageException("Unknown command: %s", name);
  }

  static AbstractCommand newInstance(
      Class<? extends AbstractCommand> commandClass, CommandContext context) throws ReflectiveOperationException {
    Constructor<? extends AbstractCommand> constructor =
        commandClass.getDeclaredConstructor(CommandContext.class);
    return constructor.newInstance(context);
  }

  public static int run(CommandContext context, String... args) throws Exception {
    try {
      if (args.length == 0) {
        throw new InvalidUsageException("No command specified.");
      }

      Class<? extends AbstractCommand> commandClass = Commands.forName(args[0]);
      AbstractCommand command = newInstance(commandClass, context);
      return command.run(Arrays.copyOfRange(args, 1, args.length));
    } catch (InvalidUsageException e) {
      e.emit(context);
      return -1;
    }
  }

  static List<Command> getCommandMeta() {
    List<Command> list = new ArrayList<>();
    for (Class<? extends AbstractCommand> command : ALL_COMMANDS) {
      list.add(command.getAnnotation(Command.class));
    }
    return list;
  }
}

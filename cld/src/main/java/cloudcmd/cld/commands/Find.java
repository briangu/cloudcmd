package cloudcmd.cld.commands;


import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;

@SubCommand(name = "find", description = "Query the index of archived files.")
public class Find implements Command
{

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
  }
}

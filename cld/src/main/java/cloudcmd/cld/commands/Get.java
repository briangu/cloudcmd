package cloudcmd.cld.commands;


import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;

@SubCommand(name = "get", description = "Export files from the cloud.")
public class Get implements Command
{

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
  }
}

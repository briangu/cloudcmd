package cloudcmd.cld.commands;


import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;

@SubCommand(name="sync", description="Syncronize the storage endpoints.")
public class Sync implements Command {
	
	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}

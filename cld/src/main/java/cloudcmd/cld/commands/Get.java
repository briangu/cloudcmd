package cloudcmd.cld.commands;


import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.CommandError;
import jpbetz.cli.SubCommand;

@SubCommand(name="reindex", description="Rebuild the index from the archived files.")
public class Get implements Command {
	
	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}

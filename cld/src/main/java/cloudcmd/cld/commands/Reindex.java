package cloudcmd.cld.commands;


import jpbetz.cli.*;

@SubCommand(name="reindex", description="Rebuild the index from the archived files.")
public class Reindex implements Command {
	
	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}

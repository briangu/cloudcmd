package cloudcmd.cld.commands;


import cloudcmd.cld.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.SubCommand;

@SubCommand(name = "reindex", description = "Rebuild the index from the archived files.")
public class Reindex implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    System.err.println("rebuilding index from local cache");
    CloudEngineService.instance().reindex();
  }
}

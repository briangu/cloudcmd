package cloudcmd.cld.commands;


import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;

@SubCommand(name = "reindex", description = "Rebuild the index from the archived files.")
public class Reindex implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    IndexStorageService.instance().purge();

    CloudEngineService.instance().reindex();
  }
}

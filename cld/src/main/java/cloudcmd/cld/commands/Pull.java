package cloudcmd.cld.commands;


import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;

@SubCommand(name = "pull", description = "Pull the meta data from storage endpoints.")
public class Pull implements Command
{

  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "b", longOpt = "blocks", description = "retrieve blocks as well as meta data", required = false)
  boolean _retrieveBlocks = true;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().pull(_maxTier.intValue(), _retrieveBlocks);
  }
}

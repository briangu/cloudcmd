package cloudcmd.cld.commands;


import cloudcmd.common.engine.BlockCacheService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;

@SubCommand(name = "update", description = "update cached adapter information.")
public class Update implements Command
{
  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    BlockCacheService.instance().refreshCache(_maxTier.intValue());
  }
}

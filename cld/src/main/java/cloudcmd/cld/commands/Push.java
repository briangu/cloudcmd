package cloudcmd.cld.commands;


import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;

@SubCommand(name = "push", description = "Push the local cache to storage endpoints.")
public class Push implements Command
{

  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    CloudEngineService.instance().push(_maxTier.intValue());
  }
}

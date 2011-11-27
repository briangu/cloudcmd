package cloudcmd.cld.commands;


import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;

@SubCommand(name="sync", description="Syncronize the storage endpoints.")
public class Push implements Command {

  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  int _maxTier = Integer.MAX_VALUE;

	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
    CloudEngineService.instance().sync(_maxTier);
  }
}

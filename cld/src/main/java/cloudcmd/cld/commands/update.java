package cloudcmd.cld.commands;


import cloudcmd.common.FileHandler;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;

import java.io.File;
import java.util.HashSet;
import java.util.List;

@SubCommand(name = "update", description = "update cached adapter information.")
public class update implements Command
{
  @Opt(opt = "t", longOpt = "tier", description = "max tier to push to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    BlockCacheService.instance().refreshCache(_maxTier.intValue());
  }
}

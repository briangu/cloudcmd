package cloudcmd.cld.commands;


import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.BlockCacheService;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;

import java.net.URI;
import java.util.List;

@SubCommand(name = "update", description = "update cached adapter information.")
public class Update implements Command
{
  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Opt(opt = "a", longOpt = "adapter", description = "adapter URI to refresh", required = false)
  String _uri = null;


  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    if (_uri == null) {
      System.err.println("updating all adapters");
      BlockCacheService.instance().refreshCache(_minTier.intValue(), _maxTier.intValue());
    } else {
      URI adapterURI = new URI(_uri);
      for (cloudcmd.common.adapters.Adapter adapter : ConfigStorageService.instance().getAdapters()) {
        if (adapter.URI.toString().equals(_uri) || (adapterURI.getPath().equals(adapter.URI.getPath()))) {
          System.err.println("updating adapter: " + adapter.URI.toString());
          adapter.refreshCache();
        }
      }
    }
  }
}

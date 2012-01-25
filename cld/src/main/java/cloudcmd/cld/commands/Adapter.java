package cloudcmd.cld.commands;


import cloudcmd.common.config.ConfigStorageService;
import jpbetz.cli.*;

import java.net.URI;

@SubCommand(name = "adapter", description = "Manager storage adapters.")
public class Adapter implements Command
{
  @Opt(opt = "r", longOpt = "remove", description = "remove adapter", required = false)
  boolean _remove = false;

  @Arg(name = "adapter URI", optional = false)
  String _uri;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    URI uri = new URI(_uri);

    if (_remove)
    {
      boolean found = ConfigStorageService.instance().removeAdapter(uri);
      if (!found)
      {
        System.err.println("could not find adapter to remove: " + uri);
      }
    }
    else
    {
      ConfigStorageService.instance().addAdapter(uri);
    }

    ConfigStorageService.instance().writeConfig();
  }
}

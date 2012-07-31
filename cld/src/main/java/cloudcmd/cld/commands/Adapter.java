package cloudcmd.cld.commands;


import cloudcmd.cld.ConfigStorageService;
import jpbetz.cli.*;

import java.net.URI;

@SubCommand(name = "adapter", description = "Manager storage adapters.")
public class Adapter implements Command
{
  @Opt(opt = "r", longOpt = "remove", description = "remove adapter", required = false)
  boolean _remove = false;

  @Opt(opt = "l", longOpt = "list", description = "list adapters", required = false)
  boolean _list = false;

  @Opt(opt = "a", longOpt = "add", description = "add adapter", required = false)
  boolean _add = false;

  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false)
  String _uri;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    if (_remove)
    {
      if (_uri == null)
      {
        System.err.println("adapter URI not specified.");
        return;
      }

      URI uri = new URI(_uri);

      boolean found = ConfigStorageService.instance().removeAdapter(uri);
      if (!found)
      {
        System.err.println("could not find adapter to remove: " + uri);
        return;
      }

      ConfigStorageService.instance().writeConfig();
    }
    else if (_add)
    {
      if (_uri == null)
      {
        System.err.println("adapter URI not specified.");
        return;
      }

      URI uri = new URI(_uri);

      ConfigStorageService.instance().addAdapter(uri);
      ConfigStorageService.instance().writeConfig();
    }
    else if (_list)
    {
      System.out.println("Adapters:");
      System.out.println();

      for (cloudcmd.common.adapters.Adapter adapter : ConfigStorageService.instance().getAdapters())
      {
        System.out.println("Adapter: " + adapter.Type());
        System.out.println("  URI: " + adapter.URI().toString());
        System.out.println("  ConfigDir: " + adapter.ConfigDir());
        System.out.println("  IsOnline: " + adapter.IsOnLine());
        System.out.println("  IsFull: " + adapter.IsFull());
        System.out.println();
      }
    }
  }
}

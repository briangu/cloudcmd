package cloudcmd.cld;

import cloudcmd.cld.commands.Index;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.CommandSet;

public class Main {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {

    ConfigStorageService.instance().init();
    IndexStorageService.instance().init();

    try
    {
      CommandSet app = new CommandSet("cld");
      app.addSubCommands(Index.class);
      app.invoke(args);
    }
    finally
    {
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();
    }
	}
}

package cloudcmd.cld;

import cloudcmd.cld.commands.Index;
import cloudcmd.common.IndexStorage;
import cloudcmd.common.IndexStorageService;
import jpbetz.cli.CommandSet;

public class Main {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {

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
    }
	}
}

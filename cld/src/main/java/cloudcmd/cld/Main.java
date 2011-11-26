package cloudcmd.cld;

import cloudcmd.cld.commands.Index;
import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.CommandSet;

import java.io.File;

public class Main {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception
  {
    String configRoot = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory(), ".cld");

    if (configRoot == null)
    {
      System.err.println("Not in a cloudcmd project.  Please use cld init to create a project.");
      return;
    }

    ConfigStorageService.instance().init();

    try
    {
      IndexStorageService.instance().init();

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

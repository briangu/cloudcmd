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
      configRoot = System.getenv("HOME") + File.separator + ".cld";
      new File(configRoot).mkdir();
    }

    ConfigStorageService.instance().init(configRoot);

    try
    {
      IndexStorageService.instance().init(configRoot);

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

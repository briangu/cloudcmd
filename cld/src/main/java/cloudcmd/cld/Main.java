package cloudcmd.cld;

import cloudcmd.cld.commands.*;
import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.CommandSet;

import java.io.File;

public class Main
{

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
      CloudEngineService.instance().init();
      IndexStorageService.instance().init();

      CommandSet app = new CommandSet("cld");
      app.addSubCommands(Index.class);
      app.addSubCommands(Reindex.class);
      app.addSubCommands(Find.class);
      app.addSubCommands(Get.class);
      app.addSubCommands(Init.class);
      app.addSubCommands(Pull.class);
      app.addSubCommands(Push.class);
      app.addSubCommands(Tag.class);
      app.invoke(args);

//      System.in.read();
    }
    finally
    {
      CloudEngineService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();
    }
  }
}

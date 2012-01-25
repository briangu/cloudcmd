package cloudcmd.cld;

import cloudcmd.cld.commands.*;
import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.BlockCacheService;
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

    try
    {
      ConfigStorageService.instance().init(configRoot);
      IndexStorageService.instance().init();
      BlockCacheService.instance().init();

      CommandSet app = new CommandSet("cld");
      app.addSubCommands(Adapter.class);
      app.addSubCommands(Find.class);
      app.addSubCommands(Get.class);
      app.addSubCommands(Index.class);
      app.addSubCommands(Init.class);
      app.addSubCommands(Listing.class);
      app.addSubCommands(Print.class);
      app.addSubCommands(Pull.class);
      app.addSubCommands(Push.class);
      app.addSubCommands(Reindex.class);
      app.addSubCommands(Tag.class);
      app.addSubCommands(Verify.class);
      app.addSubCommands(Remove.class);
      app.invoke(args);

      CloudEngineService.instance().run();
    }
    finally
    {
      CloudEngineService.instance().shutdown();
      BlockCacheService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();
    }
  }
}

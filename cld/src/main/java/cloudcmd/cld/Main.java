package cloudcmd.cld;

import cloudcmd.cld.commands.*;
import cloudcmd.common.FileUtil;
import cloudcmd.cld.ConfigStorageService;
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
      IndexStorageService.instance().init(configRoot);
      CloudEngineService.instance().init(ConfigStorageService.instance());

      CommandSet app = new CommandSet("cld");
      app.addSubCommands(Adapter.class);
      app.addSubCommands(Find.class);
      app.addSubCommands(Get.class);
      app.addSubCommands(Index.class);
      app.addSubCommands(Init.class);
      app.addSubCommands(Listing.class);
      app.addSubCommands(Print.class);
      app.addSubCommands(Reindex.class);
      app.addSubCommands(Remove.class);
      app.addSubCommands(Sync.class);
      app.addSubCommands(Tag.class);
      app.addSubCommands(Update.class);
      app.addSubCommands(Verify.class);
      app.invoke(args);

      CloudEngineService.instance().run();
    }
    finally
    {
      CloudEngineService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();
    }
  }
}

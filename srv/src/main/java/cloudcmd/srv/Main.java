package cloudcmd.srv;


import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import io.viper.core.server.Util;


public class Main
{
  public static void main(String[] args)
  {
    CloudCmdServer cloudCmdServer = null;

    try
    {
      String configRoot = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory(), ".cld");

      String staticFileRoot = String.format("%s/src/main/resources/public", Util.getCurrentWorkingDirectory());

      if (args.length >= 1) configRoot = args[0];
      if (args.length >= 2) staticFileRoot = args[1];

      if (configRoot == null)
      {
        throw new RuntimeException("cloudcmd config root directory not found or specified.");
      }

      ConfigStorageService.instance().init(configRoot);
      IndexStorageService.instance().init();
      CloudEngineService.instance().init(
        ConfigStorageService.instance().getBlockCache(),
        ConfigStorageService.instance().getReplicationStrategy(),
        "index.ops");

      cloudCmdServer = CloudCmdServer.create("localhost", 3000, staticFileRoot);
      System.in.read();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      CloudEngineService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();

      if (cloudCmdServer != null)
      {
        cloudCmdServer.shutdown();
      }
    }
  }
}
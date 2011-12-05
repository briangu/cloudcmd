package cloudcmd.srv;


import java.io.File;
import java.io.IOException;

import cloudcmd.common.FileUtil;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import io.viper.core.server.Util;
import org.json.JSONException;


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
      BlockCacheService.instance().init();
      CloudEngineService.instance().init();

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
      BlockCacheService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();

      if (cloudCmdServer != null)
      {
        cloudCmdServer.shutdown();
      }
    }
  }
}
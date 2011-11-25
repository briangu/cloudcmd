package cloudcmd.srv;


import java.io.IOException;
import io.viper.core.server.Util;
import org.json.JSONException;


public class Main
{
  public static void main(String[] args)
  {
    CloudCmdServer cloudCmdServer;

    try
    {
      String staticFileRoot = String.format("%s/src/main/resources/public", Util.getCurrentWorkingDirectory());
      String fileStorageRoot = String.format("%s/src/main/resources/public/storage/", Util.getCurrentWorkingDirectory());

      if (args.length == 3)
      {
        staticFileRoot = args[0];
        fileStorageRoot = args[1];
      }

      cloudCmdServer = CloudCmdServer.create("localhost", 3000, staticFileRoot, fileStorageRoot);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}

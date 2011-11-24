package io.viper.app.photon;


import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import io.viper.net.server.Util;
import org.json.JSONException;


public class Main
{
  public static void main(String[] args)
  {
    PhotoServer photoServer;

    try
    {
      String staticFileRoot = String.format("%s/src/main/resources/public", Util.getCurrentWorkingDirectory());

      if (args.length >= 3)
      {
        String awsId = args[0];
        String awsSecret = args[1];
        String bucketName = args[2];
        photoServer = PhotoServer.createWithS3("eat1-app54.corp.linkedin.com", 3000, awsId, awsSecret, bucketName, staticFileRoot);
      }
      else
      {
        new File(staticFileRoot).mkdir();
        photoServer = PhotoServer.create("eat1-app54.corp.linkedin.com", 3000, staticFileRoot);
      }
    }
    catch (URISyntaxException e)
    {
      e.printStackTrace();
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

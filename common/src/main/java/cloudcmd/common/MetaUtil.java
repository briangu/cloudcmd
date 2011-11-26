package cloudcmd.common;

import io.viper.core.server.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

public class MetaUtil
{
  public static JSONObject createMeta(File file, String type, String[] tags)
  {
    JSONObject obj;

    try
    {
      String fileName = file.getName();
      int extIndex = fileName.lastIndexOf(".");

      obj = Util.createJson(
        "hash", CryptoUtil.computeHash(file),
        "path", file.getCanonicalPath(),
        "filename", fileName,
        "fileext", extIndex >= 0 ? fileName.substring(extIndex) : null,
        "filesize", file.length(),
        "filedate", file.lastModified(),
        "type", type,
        "tags", tags
      );

      return obj;
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      obj = null;
    }
    catch (IOException e)
    {
      e.printStackTrace();
      obj = null;
    }

    return obj;
  }


  private static long getFileSize(File file)
  {
    return file.length();
  }
}

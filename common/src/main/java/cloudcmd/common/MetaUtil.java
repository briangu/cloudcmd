package cloudcmd.common;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class MetaUtil
{
  public static JSONObject createMeta(File file, Set<String> tags)
  {
    JSONObject obj;

    try
    {
      String fileName = file.getName();
      int extIndex = fileName.lastIndexOf(".");

      String hash = CryptoUtil.computeHashAsString(file);
      if (hash == null)
      {
        System.err.println("failed to compute hash of " + file.getAbsolutePath());
        return null;
      }

      obj = JsonUtil.createJson(
        "hash", hash,
        "path", file.getCanonicalPath(),
        "filename", fileName,
        "fileext", extIndex >= 0 ? fileName.substring(extIndex) : null,
        "filesize", file.length(),
        "filedate", file.lastModified()
      );

      if (tags != null) obj.put("tags", tags);

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

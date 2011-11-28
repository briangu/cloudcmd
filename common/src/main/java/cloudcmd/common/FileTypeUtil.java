package cloudcmd.common;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class FileTypeUtil
{
  private static FileTypeUtil _instance = null;

  private JSONObject _typeMap = null;

  public static FileTypeUtil instance()
  {
    if (_instance == null)
    {
      synchronized (FileTypeUtil.class)
      {
        if (_instance == null)
        {
          FileTypeUtil ftu = new FileTypeUtil();

          try
          {
            ftu._typeMap = ResourceUtil.loadJson("filetypes.json");
          }
          catch (IOException e)
          {
            e.printStackTrace();
          }
          catch (JSONException e)
          {
            e.printStackTrace();
          }

          _instance = ftu;
        }
      }
    }

    return _instance;
  }

  public String getTypeFromExtension(String ext)
  {
    ext = ext.toLowerCase();
    try
    {
      return _typeMap.has(ext) ? _typeMap.getString(ext) : "default";
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    return "default";
  }
}

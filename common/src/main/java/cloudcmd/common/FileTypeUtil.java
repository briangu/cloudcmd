package cloudcmd.common;

import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class FileTypeUtil
{
  private static FileTypeUtil _instance = null;

  private JSONObject _typeMap = null;
  private Set<String> _skipDir = null;
  private Set<String> _skipExt = null;

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
            JSONObject skipMap = ResourceUtil.loadJson("skipMap.json");
            ftu._skipDir = JsonUtil.createSet(skipMap.getJSONArray("dirs"));
            ftu._skipExt = JsonUtil.createSet(skipMap.getJSONArray("exts"));
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

  public boolean skipDir(String dir)
  {
    return _skipDir.contains(dir);
  }

  public boolean skipExt(String ext)
  {
    return _skipExt.contains(ext);
  }

  public String getTypeFromExtension(String ext)
  {
    ext = ext.toLowerCase();
    try
    {
      return _typeMap.has(ext) ? _typeMap.getString(ext) : null;
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    return null;
  }
}

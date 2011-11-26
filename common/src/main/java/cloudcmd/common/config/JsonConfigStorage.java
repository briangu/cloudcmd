package cloudcmd.common.config;

import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.ResourceUtil;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

public class JsonConfigStorage implements ConfigStorage
{
  private JSONObject _config = null;

  @Override
  public void init()
  {
    try
    {
      _config = ResourceUtil.loadJson("config.json");

      String home = System.getenv("HOME");
      if (home != null && !home.isEmpty())
      {
        File file = new File(home + File.separatorChar + ".cld" + File.separatorChar + "config.json");
        if (file.exists())
        {
          JSONObject overrides = FileUtil.readJson(file);
          JsonUtil.mergeLeft(_config, overrides);
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
  }

  @Override
  public void shutdown()
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getProperty(String key)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Integer getPropertyAsInt(String key)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}

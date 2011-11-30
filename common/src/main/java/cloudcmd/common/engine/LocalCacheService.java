package cloudcmd.common.engine;


import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.adapters.FileAdapter;
import cloudcmd.common.config.ConfigStorageService;
import java.io.File;
import java.util.HashSet;
import org.json.JSONException;
import org.json.JSONObject;


public class LocalCacheService
{
  private static Adapter _instance = null;

  public static Adapter instance()
  {
    if (_instance == null)
    {
      synchronized (LocalCacheService.class)
      {
        if (_instance == null)
        {
          JSONObject obj = new JSONObject();
          try
          {
            obj.put("rootPath", ConfigStorageService.instance().getConfigRoot() + File.separator + "cache");
            FileAdapter localCache = new FileAdapter();
            localCache.init(0, FileAdapter.class.getName(), new HashSet<String>(), obj);
            _instance = localCache;
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
    }

    return _instance;
  }
}

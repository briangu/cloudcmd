package cloudcmd.common.engine;


import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.adapters.FileAdapter;
import cloudcmd.common.config.ConfigStorageService;
import java.io.File;
import java.util.HashSet;
import org.json.JSONException;
import org.json.JSONObject;


public class BlockCacheService
{
  private static BlockCache _instance = null;

  public static BlockCache instance()
  {
    if (_instance == null)
    {
      synchronized (BlockCacheService.class)
      {
        if (_instance == null)
        {
          BlockCache blockCache = new LocalBlockCache();

          try
          {
            blockCache.init();
            _instance = blockCache;
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

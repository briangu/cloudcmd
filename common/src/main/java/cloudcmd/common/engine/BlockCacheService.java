package cloudcmd.common.engine;


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
          _instance = new LocalBlockCache();
        }
      }
    }

    return _instance;
  }
}

package cloudcmd.common.engine;

public class CloudEngineService
{
  private static CloudEngine _instance = null;

  public static CloudEngine instance()
  {
    if (_instance == null)
    {
      synchronized (CloudEngineService.class)
      {
        if (_instance == null)
        {
          _instance = new LocalCacheCloudEngine();
        }
      }
    }

    return _instance;
  }
}

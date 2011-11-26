package cloudcmd.common.config;

public class ConfigStorageService
{
  private static ConfigStorage _instance = null;

  public static ConfigStorage instance()
  {
    if (_instance == null)
    {
      synchronized (ConfigStorageService.class)
      {
        if (_instance == null)
        {
          _instance = new JsonConfigStorage();
        }
      }
    }

    return _instance;
  }
}

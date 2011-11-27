package cloudcmd.common.index;

public class IndexStorageService
{
  private static IndexStorage _instance = null;

  public static IndexStorage instance()
  {
    if (_instance == null)
    {
      synchronized (IndexStorageService.class)
      {
        if (_instance == null)
        {
          _instance = new H2IndexStorage();
        }
      }
    }

    return _instance;
  }
}

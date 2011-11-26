package cloudcmd.common.config;

public interface ConfigStorage
{
  void init();
  void shutdown();

  String getProperty(String key);
  Integer getPropertyAsInt(String key);

}

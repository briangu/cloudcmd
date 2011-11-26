package cloudcmd.common.config;

import cloudcmd.common.adapters.Adapter;

import java.util.List;

public interface ConfigStorage
{
  void init(String configRoot) throws Exception;
  void shutdown();

  String getProperty(String key);
  Integer getPropertyAsInt(String key);

  List<Adapter> getAdapters();

}

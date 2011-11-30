package cloudcmd.common.engine;


import cloudcmd.common.adapters.Adapter;
import java.util.List;
import java.util.Map;


public interface LocalCache
{
  Map<String, List<Adapter>> hashProviders = (Map<String, List<Adapter>>)args[0];

  void refreshCache();


}

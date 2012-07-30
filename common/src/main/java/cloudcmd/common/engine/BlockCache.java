package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;

import java.util.List;
import java.util.Map;

public interface BlockCache
{
  Map<String, List<Adapter>> getHashProviders();

  Map<String, List<Adapter>> loadCache(int minTier, int maxTier) throws Exception;

  void refreshCache(int minTier, int maxTier) throws Exception;

  void init() throws Exception;

  void shutdown();

}

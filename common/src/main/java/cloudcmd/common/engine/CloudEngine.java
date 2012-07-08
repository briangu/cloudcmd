package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import org.json.JSONArray;

import java.io.File;
import java.util.Set;


public interface CloudEngine
{
  void init(ReplicationStrategy replicationStrategy) throws Exception;

  void init(ReplicationStrategy replicationStrategy, String opsName) throws Exception;

  void prepareFlushToAdapter(Adapter adapter) throws Exception;
  
  void run() throws Exception;

  void shutdown();

  void add(File file, Set<String> tags);

  void push(int minTier, int maxTier)
      throws Exception;

  void push(int minTier, int maxTier, JSONArray selections)
    throws Exception;

  void pull(int minTier, int maxTier, boolean retrieveBlocks)
      throws Exception;

  void pull(int minTier, int maxTier, boolean retrieveBlocks, JSONArray selections)
    throws Exception;

  void reindex() throws Exception;

  void fetch(int minTier, int maxTier, JSONArray selections) throws Exception;

  JSONArray addTags(JSONArray selections, Set<String> tags) throws Exception;

  void verify(int minTier, int maxTier, boolean deleteOnInvalid) throws Exception;

  void verify(int minTier, int maxTier, JSONArray selections, boolean deleteOnInvalid) throws Exception;

  void remove(int minTier, int maxTier, JSONArray selections) throws Exception;
}

package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import org.json.JSONArray;

import java.io.File;
import java.util.Set;


public interface CloudEngine
{
  void init(ReplicationStrategy replicationStrategy) throws Exception;

  void init(ReplicationStrategy replicationStrategy, String opsName) throws Exception;

  void prepareFlushToAdapter(Adapter adpter) throws Exception;
  
  void run() throws Exception;

  void shutdown();

  void add(File file, Set<String> tags);

  void push(int maxTier)
      throws Exception;

  void push(int maxTier, JSONArray selections)
    throws Exception;

  void pull(int maxTier, boolean retrieveBlocks)
      throws Exception;

  void pull(int maxTier, boolean retrieveBlocks, JSONArray selections)
    throws Exception;

  void reindex()
      throws Exception;

  void fetch(int maxTier, JSONArray selections) throws Exception;

  JSONArray addTags(JSONArray selections, Set<String> tags)
      throws Exception;

  void verify(int i, boolean deleteOnInvalid) throws Exception;

  void verify(int i, JSONArray selections, boolean deleteOnInvalid) throws Exception;

  void remove(JSONArray selections) throws Exception;
}

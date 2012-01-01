package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import org.json.JSONArray;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;


public interface CloudEngine
{
  void init() throws Exception;

  void run() throws Exception;

  void shutdown();

  void add(File file, Set<String> tags);

  void push(int maxTier)
      throws Exception;

  void pull(int maxTier, boolean retrieveBlocks)
      throws Exception;

  void reindex()
      throws Exception;

  void fetch(int maxTier, JSONArray selections) throws Exception;

  void push(int maxTier, JSONArray selections);

  void pull(int maxTier, boolean retrieveBlocks, JSONArray selections);

  JSONArray addTags(JSONArray selections, Set<String> tags)
      throws Exception;
}

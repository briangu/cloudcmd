package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import org.json.JSONArray;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  void fetch(JSONArray selections) throws Exception;

  void push(int i, JSONArray selections);

  void pull(int i, boolean retrieveBlocks, JSONArray selections);
}

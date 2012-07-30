package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorage;
import cloudcmd.common.index.IndexStorage;
import org.json.JSONArray;

import java.io.File;
import java.util.Set;


public interface CloudEngine
{
  void init(ConfigStorage configService, IndexStorage indexStorage) throws Exception;

  void run() throws Exception;
  void shutdown();

  void reindex() throws Exception;

  void add(File file, Set<String> tags, Adapter adapter);
  void batchAdd(Set<File> file, Set<String> tags, Adapter adapter);

  void sync(int minTier, int maxTier, JSONArray selections) throws Exception;

  void fetch(int minTier, int maxTier, JSONArray selections) throws Exception;
  void verify(int minTier, int maxTier, JSONArray selections, boolean deleteOnInvalid) throws Exception;
  void remove(int minTier, int maxTier, JSONArray selections) throws Exception;

  JSONArray addTags(JSONArray selections, Set<String> tags) throws Exception;
}

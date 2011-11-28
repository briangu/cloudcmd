package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Set;

public interface IndexStorage
{
  void init() throws Exception;

  void purge();

  void flush();

  void shutdown();

  void add(FileMetaData meta);

  JSONArray find(JSONObject filter);

  void addTags(JSONArray array, Set<String> tags);

  void removeTags(JSONArray array, Set<String> tags);
}

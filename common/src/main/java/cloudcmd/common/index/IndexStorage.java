package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Set;

public interface IndexStorage
{
  void init();
  void purge();
  void flush();
  void shutdown();

  void add(FileMetaData meta);

  JSONArray find(JSONObject filter);

  Set<String> getTags(String hash);

  void addTag(JSONArray array, Set<String> tags);
  void removeTag(JSONArray array, Set<String> tags);
}

package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Set;

public interface IndexStorage
{
  // query
  //  find by tag
  //  find by other properties
  // init table
  // purge table
  // insert
  // flush
  // shutdown
  // add tag and batch
  // rm tag and batch

  void init();
  void purge();
  void flush();
  void shutdown();

  void add(FileMetaData meta);

  JSONArray find(JSONObject filter);

  void addTag(JSONArray array, Set<String> tags);
  void removeTag(JSONArray array, Set<String> tags);
}

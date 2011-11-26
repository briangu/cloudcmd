package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONObject;

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

  void add(JSONObject meta);

  JSONArray find(JSONObject filter);

  void addTag(JSONArray array, String[] tags);
  void removeTag(JSONArray array, String[] tags);
}

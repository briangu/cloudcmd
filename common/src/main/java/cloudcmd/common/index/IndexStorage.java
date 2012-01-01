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

  JSONArray addTags(JSONArray selections, Set<String> tags)
      throws Exception;

  void addAll(List<FileMetaData> fmds);

  void pruneHistory();
}

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

  JSONArray find(JSONObject filter);

  void add(FileMetaData meta);

  void remove(FileMetaData meta);

  void addAll(List<FileMetaData> fmds);

  void pruneHistory(JSONArray selections);
}

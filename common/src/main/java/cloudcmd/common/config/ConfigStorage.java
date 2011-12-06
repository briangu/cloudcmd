package cloudcmd.common.config;

import cloudcmd.common.adapters.Adapter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public interface ConfigStorage
{
  void init(String configRoot) throws Exception;

  void shutdown();

  String getConfigRoot();

  String getProperty(String key);

  Integer getPropertyAsInt(String key);

  List<Adapter> getAdapters();

  void addAdapter(URI adapterUri);

  boolean removeAdapter(URI adapterUri);

  boolean isDebugEnabled();

  void createDefaultConfig(String path) throws IOException;

  void writeConfig() throws IOException, JSONException;
}

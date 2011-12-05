package cloudcmd.common.config;

import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.ResourceUtil;
import cloudcmd.common.adapters.Adapter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class JsonConfigStorage implements ConfigStorage
{
  private static final String CONFIG_FILE = "config.json";

  private String _configRoot;
  private JSONObject _config = null;
  private List<Adapter> _adapters;
  private static final Integer DEFAULT_TIER = 1;
  private boolean _isDebug = false;

  private static String getConfigFile(String path)
  {
    return path + File.separator + CONFIG_FILE;
  }

  private static JSONObject loadConfig(String configRoot)
  {
    JSONObject config = null;

    try
    {
      config = ResourceUtil.loadJson(CONFIG_FILE);
      if (config == null)
      {
        config = new JSONObject();
      }

      File file = new File(getConfigFile(configRoot));
      if (file.exists())
      {
        JSONObject overrides = FileUtil.readJson(file);
        JsonUtil.mergeLeft(config, overrides);
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }

    return config;
  }

  private static Set<String> getTags(JSONObject config) throws JSONException
  {
    Set<String> tags = new HashSet<String>();

    if (!config.has("tags")) return tags;

    JSONArray tagConfig = config.getJSONArray("tags");

    for (int i = 0; i < tagConfig.length(); i++)
    {
      tags.add(tagConfig.getString(i));
    }

    return tags;
  }

  private static List<Adapter> loadAdapters(JSONObject config) throws JSONException, ClassNotFoundException
  {
    if (!config.has("adapters")) throw new IllegalArgumentException("config is missing the adapters field");

    JSONArray adapterConfigs = config.getJSONArray("adapters");

    List<Adapter> adapters = new ArrayList<Adapter>();

    Integer defaultTier = config.has("defaultTier") ? config.getInt("defaultTier") : DEFAULT_TIER;

    for (int i = 0; i < adapterConfigs.length(); i++)
    {
      JSONObject adapterConfig = adapterConfigs.getJSONObject(i);

      Integer tier = adapterConfig.has("tier") ? adapterConfig.getInt("tier") : defaultTier;
      Set<String> tags = getTags(adapterConfig);
      String type = adapterConfig.getString("type");
      JSONObject adapterSubConfig = adapterConfig.getJSONObject("config");

      Class<?> clazz = JsonConfigStorage.class.getClassLoader().loadClass(type);
      try
      {
        Adapter adapter = (Adapter) clazz.newInstance();
        adapter.init(tier, type, tags, adapterSubConfig);
        adapters.add(adapter);
      }
      catch (Exception e)
      {
        throw new IllegalArgumentException("unsupported adapter type found: " + type);
      }
    }

    return adapters;
  }
  private static boolean loadDebug(JSONObject config) throws JSONException
  {
    if (!config.has("debug")) return false;

    return config.getBoolean("debug");
  }


  @Override
  public void init(String configRoot) throws Exception
  {
    _configRoot = configRoot;
    _config = loadConfig(configRoot);
    _adapters = loadAdapters(_config);
    _isDebug = loadDebug(_config);
  }

  @Override
  public void shutdown()
  {
    for (Adapter adapter : _adapters)
    {
      try
      {
        adapter.shutdown();
      }
      catch (Exception e)
      {
        System.err.println("failed to shutdown adapter: " + adapter.Type);
        System.err.println("adapter config: " + adapter.Config);
        e.printStackTrace();
      }
    }
  }

  @Override
  public String getConfigRoot()
  {
    return _configRoot;
  }

  @Override
  public String getProperty(String key)
  {
    try
    {
      return _config.getString(key);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Integer getPropertyAsInt(String key)
  {
    try
    {
      return _config.getInt(key);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public List<Adapter> getAdapters()
  {
    return Collections.unmodifiableList(_adapters);
  }

  @Override
  public boolean isDebugEnabled()
  {
    return _isDebug;
  }

  @Override
  public void createDefaultConfig(String path) throws IOException {
    String configFile = getConfigFile(path);
    try {
      FileUtil.writeFile(configFile, _config.toString(2));
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}

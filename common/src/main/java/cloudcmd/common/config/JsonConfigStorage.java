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
  private String _configRoot;
  private JSONObject _config = null;
  private List<Adapter> _adapters;

  private static JSONObject loadConfig(String configRoot)
  {
    JSONObject config = null;

    try
    {
      config = ResourceUtil.loadJson("config.json");

      File file = new File(configRoot + File.separatorChar + "config.json");
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

    for (int i = 0; i < adapterConfigs.length(); i++)
    {
      JSONObject adapterConfig = adapterConfigs.getJSONObject(i);

      Set<String> tags = getTags(adapterConfig);
      String type = adapterConfig.getString("type");
      JSONObject adapterSubConfig = adapterConfig.getJSONObject("config");

      Class<?> clazz = JsonConfigStorage.class.getClassLoader().loadClass("type");
      try
      {
        Adapter adapter = (Adapter) clazz.newInstance();
        adapter.init(type, tags, adapterSubConfig);
        adapters.add(adapter);
      }
      catch (Exception e)
      {
        throw new IllegalArgumentException("unsupported adapter type found: " + type);
      }
    }

    return adapters;
  }

  @Override
  public void init(String configRoot) throws Exception
  {
    _configRoot = configRoot;
    _config = loadConfig(configRoot);
    _adapters = loadAdapters(_config);
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
}

package cloudcmd.common.config;

import cloudcmd.common.*;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCache;
import cloudcmd.common.engine.MirrorReplicationStrategy;
import cloudcmd.common.engine.ReplicationStrategy;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.*;

public class JsonConfigStorage implements ConfigStorage {
  private static final String CONFIG_FILE = "config.json";
  private static final Integer DEFAULT_TIER = 1;

  private JSONObject _config;
  private String _configRoot;

  private boolean _isDebug = false;
  private int _defaultTier;

  private List<Adapter> _allAdapters;
  private List<Adapter> _filteredAdapters;
  private Map<String, String> _adapterHandlers;

  private BlockCache _blockCache;

  private static String getConfigFile(String path) {
    return path + File.separator + CONFIG_FILE;
  }

  private static JSONObject loadConfig(String configRoot) {
    JSONObject config = null;

    try {
      config = ResourceUtil.loadJson(CONFIG_FILE);
      if (config == null) {
        config = new JSONObject();
      }

      File file = new File(getConfigFile(configRoot));
      if (file.exists()) {
        JSONObject overrides = FileUtil.readJson(file);
        JsonUtil.mergeLeft(config, overrides);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return config;
  }

  private List<Adapter> loadAdapters(JSONObject config)
    throws JSONException, ClassNotFoundException, URISyntaxException {
    if (!config.has("adapters")) throw new IllegalArgumentException("config is missing the adapters field");

    JSONArray adapterConfigs = config.getJSONArray("adapters");

    _defaultTier = config.has("defaultTier") ? config.getInt("defaultTier") : DEFAULT_TIER;

    return loadAdapters(adapterConfigs);
  }

  private List<Adapter> loadAdapters(JSONArray adapterUris)
    throws JSONException, ClassNotFoundException, URISyntaxException {
    List<Adapter> adapters = new ArrayList<Adapter>();

    for (int i = 0; i < adapterUris.length(); i++) {
      URI adapterUri = new URI(adapterUris.getString(i));
      Adapter adapter = loadAdapter(adapterUri);
      adapters.add(adapter);
    }

    return adapters;
  }

  private Integer getTierFromUri(URI adapterUri) {
    Map<String, String> queryParams = UriUtil.parseQueryString(adapterUri);
    Integer tier = (queryParams.containsKey("tier")) ? Integer.parseInt(queryParams.get("tier")) : _defaultTier;
    return tier;
  }

  private Set<String> getTagsFromUri(URI adapterUri) {
    Set<String> tags = new HashSet<String>();
    Map<String, String> queryParams = UriUtil.parseQueryString(adapterUri);

    if (queryParams.containsKey("tags")) {
      String[] parts = queryParams.get("tags").split(",");
      for (String tag : parts) {
        tags.add(tag);
      }
    }

    return tags;
  }

  private Adapter loadAdapter(URI adapterUri) throws ClassNotFoundException {
    Adapter adapter;

    String scheme = adapterUri.getScheme();

    if (!_adapterHandlers.containsKey(scheme)) {
      throw new IllegalArgumentException(String.format("scheme %s in adapter URI %s is not supported!", scheme, adapterUri));
    }

    String handlerType = _adapterHandlers.get(scheme);
    Integer tier = getTierFromUri(adapterUri);
    Set<String> tags = getTagsFromUri(adapterUri);

    Class<?> clazz = JsonConfigStorage.class.getClassLoader().loadClass(handlerType);
    try {
      adapter = (Adapter) clazz.newInstance();
      String adapterIdHash = CryptoUtil.digestToString(CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(adapterUri.toASCIIString().getBytes("UTF-8")))));
      adapter.init(_configRoot + File.separator + "adapterCaches" + File.separator + adapterIdHash, tier, handlerType, tags, adapterUri);
    } catch (Exception e) {
      throw new RuntimeException(String.format("failed to initialize adapter %s for adapter %s", handlerType, adapterUri));
    }

    return adapter;
  }

  private static boolean loadDebug(JSONObject config) throws JSONException {
    if (!config.has("debug")) return false;

    return config.getBoolean("debug");
  }

  private static Map<String, String> loadAdapterHandlers(JSONObject config) throws JSONException {
    if (!config.has("adapterHandlers")) throw new IllegalArgumentException("config is missing the adapters field");

    JSONObject handlers = config.getJSONObject("adapterHandlers");

    Map<String, String> adapterHandlers = new HashMap<String, String>();

    Iterator<String> keys = handlers.keys();

    while (keys.hasNext()) {
      String key = keys.next();
      adapterHandlers.put(key, handlers.getString(key));
    }

    return adapterHandlers;
  }


  @Override
  public void init(String configRoot) throws Exception {
    _configRoot = configRoot;
    _config = loadConfig(configRoot);
    _adapterHandlers = loadAdapterHandlers(_config);
    _allAdapters = loadAdapters(_config);
    _filteredAdapters = _allAdapters;
    _isDebug = loadDebug(_config);
  }

  @Override
  public void shutdown() {
    if (_allAdapters == null) return;

    for (Adapter adapter : _allAdapters) {
      try {
        adapter.shutdown();
      } catch (Exception e) {
        System.err.println("failed to shutdown adapter: " + adapter.Type());
        System.err.println("adapter config: " + adapter.URI());
        e.printStackTrace();
      }
    }


  }

  @Override
  public String getConfigRoot() {
    return _configRoot;
  }

  @Override
  public String getProperty(String key) {
    try {
      return _config.getString(key);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Integer getPropertyAsInt(String key) {
    try {
      return _config.getInt(key);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public boolean isDebugEnabled() {
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

  private void rebuildConfig() throws JSONException {
    _config.put("defaultTier", _defaultTier);

    JSONArray adapters = new JSONArray();

    for (Adapter adapter : _allAdapters) {
      adapters.put(adapter.URI().toString());
    }

    _config.put("adapters", adapters);
  }

  @Override
  public void writeConfig() throws IOException, JSONException {
    rebuildConfig();

    String configFile = getConfigFile(_configRoot);
    try {
      FileUtil.writeFile(configFile, _config.toString(2));
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Adapter getAdapter(URI adapterURI) {
    List<Adapter> adapters = getAdapters();
    for (Adapter adapter : adapters) {
      if (adapter.URI().equals(adapterURI)) {
        return adapter;
      }
    }
    return null;
  }

  @Override
  public BlockCache getBlockCache() {
    return _blockCache;
  }

  @Override
  public ReplicationStrategy getReplicationStrategy() {
    return new MirrorReplicationStrategy(_blockCache);
  }

  @Override
  public boolean removeAdapter(URI uri) {
    return false;
  }

  @Override
  public List<Adapter> getAdapters() {
    return Collections.unmodifiableList(_filteredAdapters);
  }

  @Override
  public void filterAdapters(Integer minTier, Integer maxTier) {
    _filteredAdapters = new ArrayList<Adapter>();
    for (Adapter adapter : _allAdapters) {
      if (!adapter.IsFull() && adapter.IsOnLine() && (adapter.Tier() >= minTier && adapter.Tier() <= maxTier)) {
        _filteredAdapters.add(adapter);
      }
    }
  }

  @Override
  public List<Adapter> getAllAdapters() {
    return Collections.unmodifiableList(_allAdapters);
  }

  @Override
  public void addAdapter(URI adapterUri) {
    try {
      Adapter adapter = loadAdapter(adapterUri);
      _allAdapters.add(adapter);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}

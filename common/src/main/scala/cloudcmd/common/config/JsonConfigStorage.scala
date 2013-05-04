package cloudcmd.common.config

import cloudcmd.common._
import cloudcmd.common.adapters.Adapter
import cloudcmd.common.engine.MirrorReplicationStrategy
import cloudcmd.common.engine.ReplicationStrategy
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.net.URI
import java.nio.channels.Channels
import collection.mutable.ListBuffer
import util.{CryptoUtil, JsonUtil}

class JsonConfigStorage extends ConfigStorage {

  private final val CONFIG_FILE = "config.json"
  private final val DEFAULT_TIER = 1

  private var _config: JSONObject = null
  private var _configRoot: String = null
  private var _isDebug: Boolean = false
  private var _defaultTier: Int = 0
  private var _allAdapters: List[Adapter] = null
  private var _filteredAdapters: List[Adapter] = null
  private var _adapterHandlers: Map[String, String] = null

  private def getConfigFile(path: String): String = {
    path + File.separator + CONFIG_FILE
  }

  private def loadConfig(configRoot: String): JSONObject = {
    var config: JSONObject = null
    try {
      config = ResourceUtil.loadJson(CONFIG_FILE)
      if (config == null) {
        config = new JSONObject
      }
      val file = new File(getConfigFile(configRoot))
      if (file.exists) {
        val overrides = FileUtil.readJson(file)
        JsonUtil.mergeLeft(config, overrides)
      }
    }
    catch {
      case e: IOException => e.printStackTrace()
      case e: JSONException => e.printStackTrace()
    }
    config
  }

  private def loadDebug(config: JSONObject): Boolean = {
    if (config.has("debug")) config.getBoolean("debug") else false
  }

  private def loadAdapterHandlers(config: JSONObject): Map[String, String] = {
    if (!config.has("adapterHandlers")) throw new IllegalArgumentException("config is missing the adapters field")
    JsonUtil.toStringMap(config.getJSONObject("adapterHandlers"))
  }

  private def loadAdapters(config: JSONObject): List[Adapter] = {
    if (!config.has("adapters")) throw new IllegalArgumentException("config is missing the adapters field")
    val adapterConfigs = config.getJSONArray("adapters")
    _defaultTier = if (config.has("defaultTier")) config.getInt("defaultTier") else DEFAULT_TIER
    loadAdapters(adapterConfigs)
  }

  private def loadAdapters(adapterUris: JSONArray): List[Adapter] = {
    val adapters = new ListBuffer[Adapter]
    (0 until adapterUris.length()).foreach{ i =>
      val adapterUri = new URI(adapterUris.getString(i))
      val adapter = loadAdapter(adapterUri)
      adapters.append(adapter)
    }
    adapters.toList
  }

  private def getTierFromUri(adapterUri: URI): Int = {
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if ((queryParams.containsKey("tier"))) queryParams.get("tier").toInt else _defaultTier
  }

  private def getTagsFromUri(adapterUri: URI): Set[String] = {
    val queryParams = UriUtil.parseQueryString(adapterUri)
    if (queryParams.containsKey("tags")) {
      val parts = queryParams.get("tags").split(",").filter(_.length > 0)
      parts.flatMap(Set(_)).toSet
    } else {
      Set()
    }
  }

  private def loadAdapter(adapterUri: URI): Adapter = {
    var adapter: Adapter = null
    val scheme = adapterUri.getScheme
    if (!_adapterHandlers.contains(scheme)) {
      throw new IllegalArgumentException(String.format("scheme %s in adapter URI %s is not supported!", scheme, adapterUri))
    }
    val handlerType = _adapterHandlers.get(scheme).get
    val tier = getTierFromUri(adapterUri)
    val tags = getTagsFromUri(adapterUri)
    val clazz = classOf[JsonConfigStorage].getClassLoader.loadClass(handlerType)
    try {
      adapter = clazz.newInstance.asInstanceOf[Adapter]
      val adapterIdHash = CryptoUtil.digestToString(CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(adapterUri.toASCIIString.getBytes("UTF-8")))))
      adapter.init(_configRoot + File.separator + "adapterCaches" + File.separator + adapterIdHash, tier, handlerType, tags.toSet, adapterUri)
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(String.format("failed to initialize adapter %s for adapter %s", handlerType, adapterUri))
      }
    }
    adapter
  }

  def init(configRoot: String) {
    _configRoot = configRoot
    _config = loadConfig(configRoot)
    _adapterHandlers = loadAdapterHandlers(_config)
    _allAdapters = loadAdapters(_config)
    _filteredAdapters = _allAdapters
    _isDebug = loadDebug(_config)
  }

  def shutdown {
    if (_allAdapters == null) return
    for (adapter <- _allAdapters) {
      try {
        adapter.shutdown()
      }
      catch {
        case e: Exception => {
          System.err.println("failed to shutdown adapter: " + adapter.Type)
          System.err.println("adapter config: " + adapter.URI)
          e.printStackTrace()
        }
      }
    }
  }

  def getConfigRoot: String = _configRoot

  def getProperty(key: String): String = {
    try {
      _config.getString(key)
    }
    catch {
      case e: JSONException => {
        e.printStackTrace()
        null
      }
    }
  }

  def getPropertyAsInt(key: String): Int = {
    try {
      _config.getInt(key)
    }
    catch {
      case e: JSONException => {
        e.printStackTrace()
        -1
      }
    }
  }

  def isDebugEnabled: Boolean = _isDebug

  def createDefaultConfig(path: String) {
    val configFile: String = getConfigFile(path)
    try {
      FileUtil.writeFile(configFile, _config.toString(2))
    }
    catch {
      case e: JSONException => e.printStackTrace()
    }
  }

  private def rebuildConfig() {
    _config.put("defaultTier", _defaultTier)
    val adapters = new JSONArray
    for (adapter <- _allAdapters) {
      adapters.put(adapter.URI.toString)
    }
    _config.put("adapters", adapters)
  }

  def writeConfig {
    rebuildConfig
    val configFile = getConfigFile(_configRoot)
    try {
      FileUtil.writeFile(configFile, _config.toString(2))
    }
    catch {
      case e: JSONException => e.printStackTrace()
    }
  }

  def getAdapter(adapterURI: URI): Adapter = {
    val adapters: List[Adapter] = getAdapters
    for (adapter <- adapters) {
      if (adapter.URI == adapterURI) {
        return adapter
      }
    }
    null
  }

  def getReplicationStrategy: ReplicationStrategy = {
    new MirrorReplicationStrategy
  }

  def removeAdapter(uri: URI): Boolean = {
    false
  }

  def getAdapters: List[Adapter] = {
    _filteredAdapters
  }

  def addAdapter(adapterUri: URI) {
    try {
      val adapter: Adapter = loadAdapter(adapterUri)
      _allAdapters = _allAdapters ++ List(adapter)
    }
    catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }
  }
}
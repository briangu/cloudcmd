package cloudcmd.common.config

import cloudcmd.common._
import cloudcmd.common.adapters.{DirectAdapter, IndexedAdapter}
import cloudcmd.common.engine.MirrorReplicationStrategy
import cloudcmd.common.engine.ReplicationStrategy
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.net.URI
import collection.mutable.ListBuffer
import util.JsonUtil

class JsonConfigStorage extends ConfigStorage {

  private final val CONFIG_FILE = "config.json"
  private final val DEFAULT_TIER = 1

  private var _config: JSONObject = null
  private var _configRoot: String = null
  private var _isDebug: Boolean = false
  private var _defaultTier: Int = 0
  private var _minTier = 0
  private var _maxTier = Int.MaxValue
  private var _allAdapters: List[IndexedAdapter] = null
  private var _auxAdapters: Map[String, DirectAdapter] = null
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

  private def loadAdapters(config: JSONObject): List[IndexedAdapter] = {
    if (!config.has("adapters")) throw new IllegalArgumentException("config is missing the adapters field")
    val adapterConfigs = config.getJSONArray("adapters")
    _defaultTier = if (config.has("defaultTier")) config.getInt("defaultTier") else DEFAULT_TIER
    loadAdapters(adapterConfigs)
  }

  private def loadAdapters(adapterUris: JSONArray): List[IndexedAdapter] = {
    val adapters = new ListBuffer[IndexedAdapter]
    (0 until adapterUris.length()).foreach{ i =>
      val adapterUri = new URI(adapterUris.getString(i))
      val adapter = AdapterFactory.createIndexedAdapter(_configRoot, _adapterHandlers, adapterUri)
      adapters.append(adapter)
    }
    adapters.toList
  }

  private def loadAuxiliaryAdapters(config: JSONObject): Map[String, DirectAdapter] = {
    if (config.has("auxAdapters")) {
      import scala.collection.JavaConversions._
      val adapterConfigs = config.getJSONObject("auxAdapters")
      Map() ++ adapterConfigs.keys().flatMap { key =>
        val adapterUri = new URI(adapterConfigs.getString(key.asInstanceOf[String]))
        val adapter = AdapterFactory.createIndexedAdapter(_configRoot, _adapterHandlers, adapterUri)
        Map(key.asInstanceOf[String] -> adapter)
      }
    } else {
      Map()
    }
  }

  def init(configRoot: String) {
    _configRoot = configRoot
    _config = loadConfig(configRoot)
    _adapterHandlers = loadAdapterHandlers(_config)
    _allAdapters = loadAdapters(_config)
    _auxAdapters = loadAuxiliaryAdapters(_config)
    _isDebug = loadDebug(_config)
  }

  def shutdown() {
    val adapters = (if (_allAdapters != null) _allAdapters else List()) ++ (if (_auxAdapters != null) _auxAdapters.values.toList else List())
    for (adapter <- adapters) {
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

  def setAdapterTierRange(minTier: Int, maxTier: Int) {
    _minTier = minTier
    _maxTier = maxTier
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

  def writeConfig() {
    rebuildConfig()
    val configFile = getConfigFile(_configRoot)
    try {
      FileUtil.writeFile(configFile, _config.toString(2))
    }
    catch {
      case e: JSONException => e.printStackTrace()
    }
  }

  def getAdapter(adapterURI: URI): IndexedAdapter = {
    val adapters: List[IndexedAdapter] = getAllAdapters
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
    val adapter = findAdapterByBestMatch(uri.toASCIIString)
    val contains = _allAdapters.contains(adapter)
    if (contains) {
      _allAdapters = _allAdapters.diff(List(adapter))
    }
    contains
  }

  def getAllAdapters: List[IndexedAdapter] = {
    _allAdapters
  }

  def getAuxilaryAdapter(key: String): Option[DirectAdapter] = {
    _auxAdapters.get(key)
  }

  def getMaxAdapterTier: Int = {
    _allAdapters.map(_.Tier).max
  }

  def getMinAdapterTier: Int = {
    _allAdapters.map(_.Tier).min
  }

  def getFilteredAdapters: List[IndexedAdapter] = {
    _allAdapters.filter(a => a.Tier >= _minTier && a.Tier <= _maxTier && a.IsOnLine && !a.IsFull).toList
  }

  def addAdapter(adapterUri: URI) {
    try {
      val adapter: IndexedAdapter = AdapterFactory.createIndexedAdapter(_configRoot, _adapterHandlers, adapterUri)
      _allAdapters = _allAdapters ++ List(adapter)
    }
    catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }
  }
}
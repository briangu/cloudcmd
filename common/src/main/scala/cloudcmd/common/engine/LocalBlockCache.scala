package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.adapters.FileAdapter
import cloudcmd.common.config.{ConfigStorage, ConfigStorageService}
import java.io.File
import java.net.URI
import java.util

class LocalBlockCache(configStorage : ConfigStorage) extends BlockCache {
  private var _cacheAdapter: Adapter = null
  private var _hashProviders: java.util.Map[String, java.util.List[Adapter]] = null

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def init {
    val adapterUri: URI = new URI("file:///" + ConfigStorageService.instance.getConfigRoot + File.separator + "cache")
    _cacheAdapter = new FileAdapter(true)
    _cacheAdapter.init(null, 0, classOf[FileAdapter].getName, new java.util.HashSet[String], adapterUri)
  }

  def shutdown {}

  def getCacheAdapter: Adapter = _cacheAdapter

  def refreshCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    val adapters : List[Adapter] = ConfigStorageService.instance.getAdapters.toList ::: List(_cacheAdapter)
    adapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def loadCache(minTier: Int, maxTier: Int) {
    _hashProviders = buildHashProviders(minTier, maxTier)
  }

  private def buildHashProviders(minTier: Int, maxTier: Int) : java.util.Map[String, java.util.List[Adapter]] = {
    import scala.collection.JavaConversions._

    val adapters : List[Adapter] = ConfigStorageService.instance.getAdapters.toList ::: List(_cacheAdapter)

    val filteredAdapters = adapters.filter(available(_, minTier, maxTier))
    val am = filteredAdapters.par.flatMap(p => p.describe).toSet

    val hashProviders = new java.util.HashMap[String, java.util.List[Adapter]]

    am.par.foreach{ hash =>
      val res = new util.ArrayList[Adapter]
      filteredAdapters.foreach(adapter => if (adapter.describe().contains(hash)) res.add(adapter))
      hashProviders.synchronized {
        hashProviders.put(hash, res)
      }
    }

    hashProviders
  }

  def getHashProviders: java.util.Map[String, java.util.List[Adapter]] = {
    if (_hashProviders == null) {
      throw new RuntimeException("call loadCache first")
    }
    java.util.Collections.unmodifiableMap(_hashProviders)
  }
}
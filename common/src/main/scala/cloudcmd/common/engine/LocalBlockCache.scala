package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.adapters.FileAdapter
import cloudcmd.common.config.ConfigStorageService
import java.io.File
import java.net.URI

class LocalBlockCache extends BlockCache {
  private var _cacheAdapter: Adapter = null
  private var _hashProviders: java.util.Map[String, java.util.List[Adapter]] = null

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def init {
    val adapterUri: URI = new URI("file:///" + ConfigStorageService.instance.getConfigRoot + File.separator + "cache")
    _cacheAdapter = new FileAdapter(true)
    val cacheIndex: String = ConfigStorageService.instance.getConfigRoot + File.separator + "adapterCaches" + File.separator + "localCache"
    _cacheAdapter.init(cacheIndex, 0, classOf[FileAdapter].getName, new java.util.HashSet[String], adapterUri)
  }

  def shutdown {}

  def getBlockCache: Adapter = _cacheAdapter

  def refreshCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    val adapters : List[Adapter] = ConfigStorageService.instance.getAdapters.toList ::: List(BlockCacheService.instance.getBlockCache)
    adapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def loadCache(minTier: Int, maxTier: Int) {
    _hashProviders = buildHashProviders(minTier, maxTier)
  }

  private def buildHashProviders(minTier: Int, maxTier: Int) : java.util.Map[String, java.util.List[Adapter]] = {
    import scala.collection.JavaConversions._

    val adapters : List[Adapter] = ConfigStorageService.instance.getAdapters.toList ::: List(BlockCacheService.instance.getBlockCache)

    val hashProviders = new java.util.HashMap[String, java.util.List[Adapter]]

    adapters.filter(available(_, minTier, maxTier)).par.foreach{ p =>
      p.describe.foreach{ hash =>
        if (!hashProviders.containsKey(hash)) {
          hashProviders.synchronized {
            if (!hashProviders.containsKey(hash)) {
              hashProviders.put(hash, new java.util.ArrayList[Adapter])
            }
          }
        }
        hashProviders.get(hash).synchronized {
          hashProviders.get(hash).add(p)
        }
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
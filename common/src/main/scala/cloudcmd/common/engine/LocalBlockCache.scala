package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.adapters.FileAdapter
import cloudcmd.common.config.ConfigStorageService
import java.io.File
import java.net.URI
import java.util._

class LocalBlockCache extends BlockCache {
  private var _cacheAdapter: Adapter = null
  private var _hashProviders: Map[String, List[Adapter]] = null

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def init {
    val adapterUri: URI = new URI("file:///" + ConfigStorageService.instance.getConfigRoot + File.separator + "cache")
    _cacheAdapter = new FileAdapter
    val cacheIndex: String = ConfigStorageService.instance.getConfigRoot + File.separator + "adapterCaches" + File.separator + "localCache"
    _cacheAdapter.init(cacheIndex, 0, classOf[FileAdapter].getName, new HashSet[String], adapterUri)
  }

  def shutdown {}

  def getBlockCache: Adapter = _cacheAdapter

  def refreshCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    ConfigStorageService.instance.getAdapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def loadCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._

    _hashProviders = new HashMap[String, List[Adapter]]

    ConfigStorageService.instance.getAdapters.filter(available(_, minTier, maxTier)).par.foreach{ p =>
      p.describe.foreach{ hash =>
        if (!_hashProviders.containsKey(hash)) {
          _hashProviders.synchronized {
            if (!_hashProviders.containsKey(hash)) {
              _hashProviders.put(hash, new ArrayList[Adapter])
            }
          }
        }
        _hashProviders.get(hash).synchronized {
          _hashProviders.get(hash).add(p)
        }
      }
    }
  }

  def getHashProviders: Map[String, List[Adapter]] = {
    if (_hashProviders == null) {
      throw new RuntimeException("call loadCache first")
    }
    Collections.unmodifiableMap(_hashProviders)
  }
}
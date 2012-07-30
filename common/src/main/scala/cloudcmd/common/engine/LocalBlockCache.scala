package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.config.ConfigStorage
import java.util

class LocalBlockCache(configStorage : ConfigStorage) extends BlockCache {
  private var _hashProviders: java.util.Map[String, java.util.List[Adapter]] = null

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def init {
  }

  def shutdown {}

  def refreshCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    val adapters : List[Adapter] = configStorage.getAdapters.toList
    adapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def loadCache(minTier: Int, maxTier: Int) : java.util.Map[String, java.util.List[Adapter]] = {
    _hashProviders = buildHashProviders(minTier, maxTier)
    getHashProviders
  }

  private def buildHashProviders(minTier: Int, maxTier: Int) : java.util.Map[String, java.util.List[Adapter]] = {
    import scala.collection.JavaConversions._

    val adapters : List[Adapter] = configStorage.getAdapters.toList

    val filteredAdapters = adapters.filter(available(_, minTier, maxTier))
    val am = filteredAdapters.par.flatMap(p => p.describe).toSet

    val hashProviders = new java.util.HashMap[String, java.util.List[Adapter]]

    // TODO: this code makes me cry
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
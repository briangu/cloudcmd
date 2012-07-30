package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import cloudcmd.common.config.ConfigStorage

class LocalBlockCache(configStorage: ConfigStorage) extends BlockCache {
  private var _hashProviders: Map[String, List[Adapter]] = null

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def init {}

  def shutdown {}

  def refreshCache(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    configStorage.getAdapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def loadCache(minTier: Int, maxTier: Int): Map[String, List[Adapter]] = {
    _hashProviders = buildHashProviders(minTier, maxTier)
    getHashProviders
  }

  private def buildHashProviders(minTier: Int, maxTier: Int): Map[String, List[Adapter]] = {
    import scala.collection.JavaConversions._
    val adapters = configStorage.getAdapters.filter(available(_, minTier, maxTier))
    Map() ++ adapters.flatMap(p => p.describe.toSet).par.flatMap {
      hash => Map(hash -> adapters.filter(_.describe().contains(hash)).toList)
    }
  }

  def getHashProviders: Map[String, List[Adapter]] = {
    if (_hashProviders == null) {
      throw new RuntimeException("call loadCache first")
    }
    _hashProviders
  }
}
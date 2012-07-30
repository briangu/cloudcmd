package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter

trait BlockCache {
  def getHashProviders(): Map[String, List[Adapter]]

  def loadCache(minTier: Int, maxTier: Int): Map[String, List[Adapter]]

  def refreshCache(minTier: Int, maxTier: Int)
}